"""Schema definition and manipulation for DataGraphs domain models."""

import copy
import json
import datetime
from collections import deque
from contextlib import contextmanager
from itertools import groupby
from dataclasses import dataclass, field
from typing import Any, Generator, Optional, Self, Union
from datagraphs.enums import DATATYPE
from datagraphs.utils import SchemaTransformer


# ---------------------------------------------------------------------------
# Structural diff layer — Phase 3
# ---------------------------------------------------------------------------
#
# _diff(baseline, current, rename_map=None) -> list[Change]
#
# Computes the net-effect structural delta between two schema dicts.
# rename_map is accepted as a forward-compatible seam but is ignored here;
# it will be wired in Phase 4 (rename reconciliation).
#
# Hard invariants:
#   - createdDate / lastModifiedDate are NEVER compared or emitted.
#   - Descriptions are normalised to plain text before comparison.
#   - create-then-delete, modify-then-revert: produce no entry.
#   - O(n) via name-keyed maps for class and property lookup.
# ---------------------------------------------------------------------------

#: Top-level date fields excluded from diff everywhere.
_DATE_FIELDS: frozenset[str] = frozenset({"createdDate", "lastModifiedDate"})

#: Top-level metadata keys that are server-internal / structural plumbing and
#: must NOT surface as user-facing change-report entries.  ``name`` and the
#: ``classes`` list are handled explicitly; dates are excluded above; everything
#: else listed here (identifiers, JSON-LD context, type tags) is server-owned
#: and a change to it is not a domain-model edit the report should claim.
_METADATA_EXCLUDED_KEYS: frozenset[str] = frozenset(
    {"name", "classes", "guid", "id", "@id", "@context", "@type", "type", "uri"}
)

#: Per-class fields that are diffed (description is handled separately).
_CLASS_FIELDS: tuple[str, ...] = (
    "subClassOf",
    "labelProperty",
    "isAbstract",
    "identifierProperty",
)

#: Per-property fields diffed (description is handled separately).
_PROPERTY_FIELDS: tuple[str, ...] = (
    "isOptional",
    "isArray",
    "range",
    "type",
    "isLangString",
    "isLabelSynonym",
    "isFilterable",
    "validationRules",
    "inverseOf",
    "isNestedObject",
)


@dataclass(frozen=True)
class Change:
    """Represents a single net-effect change detected by the structural diff.

    Attributes:
        target: The entity affected, e.g. ``"metadata"``, ``"ClassName"``,
            or ``"ClassName.propName"``.
        kind: Broad category: ``"metadata"``, ``"class"``, or ``"property"``.
        op: The operation: ``"added"``, ``"removed"``, or ``"modified"``.
        from_: Before-value for scalar changes (``op="modified"`` on metadata).
        to: After-value for scalar changes.
        fields: For ``op="modified"`` class/property changes — an ordered list
            of ``{"field": str, "before": Any, "after": Any}`` dicts, one per
            changed field.  Only changed fields are included.
        detail: Optional free-form dict for supplementary annotations.
            Phase 5 uses ``{"reorder_candidate": True}`` to flag property
            sequences whose *set* is unchanged but whose *order* differs.
    """

    target: str
    kind: str
    op: str
    from_: Any = field(default=None)
    to: Any = field(default=None)
    fields: Optional[list[dict]] = field(default=None)
    detail: Optional[dict] = field(default=None)


# ---------------------------------------------------------------------------
# Event-sourced identity model (ADR 0003 — identity correspondence)
# ---------------------------------------------------------------------------
#
# Names are not identities.  A name can be freed (by rename OR delete) and then
# reused (by rename OR creation), and can be renamed in chains.  Reconstructing
# identity post-hoc from rename events alone (the prior _fold_renames) steals a
# baseline entity's identity the moment a freed name is recycled by a *new*
# entity, because the fold never sees creation events.
#
# Instead we assign every entity a STABLE IDENTITY (an opaque monotonic int) at
# the moment it enters the timeline, and REPLAY THE ENTIRE OP-LOG over the
# baseline to maintain a live "current name -> identity" map.  Creation MINTS a
# fresh identity bound to the name; rename REBINDS the existing identity's name
# (never mints); delete ENDS the identity.  A name freed then reused therefore
# binds to a *different* identity — recycle is correct by construction, whether
# reused by rename or by creation.
#
# Identities live only here, in resolver-local maps; they are NEVER stamped on
# the schema dict or the wire format (ADR 0003 forbids synthetic ids on the
# dict).  From the replay we derive, per identity, its baseline name (or None if
# born after baseline) and its final tracked name (or None if deleted).  The
# diff then matches baseline entities to current-_schema entities BY IDENTITY
# CORRESPONDENCE; entities the replay did not predict degrade to "added".
#
# Property identities are scoped by CLASS IDENTITY (not class name), so a class
# rename never orphans property identities.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RenameMap:
    """Identity correspondence derived by event-sourced op-log replay.

    Despite the historical name, this value object no longer "folds renames":
    it is the product of replaying the whole op-log over the baseline to assign
    every entity a stable identity (see :func:`_replay_identities`).  It exposes
    exactly the identity correspondence the structural diff (ADR 0001) cannot
    infer from the two dict states alone — baseline name <-> current name, by
    identity — plus per-op-log-position resolution of an op's call-time names to
    the current name of whichever entity bore that name *at the op's position*.

    Round-trips (``A->B->A``) collapse naturally (final name == baseline name =>
    no entry) and recycles bind distinct identities (a recycled name's new
    identity has no baseline name => it is *added*, never matched to the
    original baseline entity).

    Attributes:
        classes: ``{baseline_class_name -> current_class_name}`` for every
            class that survived to a *different* current name (genuine rename).
            A class born after baseline (no baseline name) never appears here.
        properties: ``{(baseline_class_name, baseline_prop_name) ->
            current_prop_name}`` for every property that survived to a different
            current name.  The key's class component is the owning class's
            *baseline* name, so a property rename stays correctly scoped even
            when its owning class was itself renamed.
        entry_class_resolution: Per op-log position, the final current name of
            whichever class bore that entry's call-time ``class_name`` at the
            entry's position (``None`` if the class did not survive, or the
            entry carries no class name).
        entry_prop_resolution: Per op-log position, the
            ``(final_class_name, final_prop_name)`` of the property the entry's
            call-time ``(class_name, prop_name)`` referred to at its position
            (``None`` if absent or not surviving).
        class_fate: ``{baseline_class_name -> Optional[final_current_name]}`` for
            EVERY class that existed at baseline.  ``None`` means the class's
            identity was ended (deleted) during the session, so a current class
            of the same name is a *different* identity (recycle), not this one.
            This is what lets the diff match by identity and never swallow a
            destructive delete behind a recycled name.
        prop_fate: ``{(baseline_class_name, baseline_prop_name) ->
            Optional[final_current_name]}`` for every property that existed at
            baseline; ``None`` means its identity was deleted.
    """

    classes: dict[str, str]
    properties: dict[tuple[str, str], str]
    entry_class_resolution: list[Optional[str]] = field(default_factory=list)
    entry_prop_resolution: list[Optional[tuple[str, str]]] = field(
        default_factory=list
    )
    class_fate: dict[str, Optional[str]] = field(default_factory=dict)
    prop_fate: dict[tuple[str, str], Optional[str]] = field(default_factory=dict)

    def class_current_name(self, baseline_class_name: str) -> str:
        """Return the current name of a class given its baseline identity.

        :param baseline_class_name: The class name as it was at baseline.
        :returns: The current class name, or *baseline_class_name* unchanged
            when the class was never renamed.
        """
        return self.classes.get(baseline_class_name, baseline_class_name)

    def property_current_name(
        self, baseline_class_name: str, baseline_prop_name: str
    ) -> str:
        """Return the current name of a property given its baseline identity.

        :param baseline_class_name: Owning class name as it was at baseline.
        :param baseline_prop_name: Property name as it was at baseline.
        :returns: The current property name, or *baseline_prop_name* unchanged
            when the property was never renamed.
        """
        return self.properties.get(
            (baseline_class_name, baseline_prop_name), baseline_prop_name
        )

    def resolve_class_at(self, position: int, call_time_class_name: str) -> str:
        """Resolve an op-log entry's call-time class name to its current name.

        Recycle-safe by construction: the resolution was captured by replaying
        the op-log, so an op issued under a name later freed and recycled by a
        different entity resolves to the entity that bore the name *at the op's
        position*.  Falls back to the call-time name when the entry carries no
        recorded resolution (covers a class deleted later, or the no-rename
        case).

        :param position: The op-log entry's index in ``Schema._change_log``.
        :param call_time_class_name: Class name as recorded in the op-log entry.
        :returns: The class's final current name.
        """
        if 0 <= position < len(self.entry_class_resolution):
            resolved = self.entry_class_resolution[position]
            if resolved is not None:
                return resolved
        return call_time_class_name

    def resolve_property_at(
        self, position: int, call_time_class_name: str, call_time_prop_name: str
    ) -> tuple[str, str]:
        """Resolve an op-log entry's call-time ``(class, prop)`` to current names.

        :param position: The op-log entry's index in ``Schema._change_log``.
        :param call_time_class_name: Owning class name as recorded in the op-log.
        :param call_time_prop_name: Property name as recorded in the op-log.
        :returns: ``(current_class_name, current_prop_name)``.
        """
        if 0 <= position < len(self.entry_prop_resolution):
            resolved = self.entry_prop_resolution[position]
            if resolved is not None:
                return resolved
        return (
            self.resolve_class_at(position, call_time_class_name),
            call_time_prop_name,
        )


# Op kinds that MINT a fresh identity bound to a name as it enters the timeline.
_CREATE_CLASS_OPS: frozenset[str] = frozenset({"create_class", "create_subclass"})


def _replay_identities(baseline: dict, change_log: list[dict]) -> RenameMap:
    """Replay the op-log over the baseline to derive identity correspondence.

    Each baseline class is seeded with a fresh class-identity and each baseline
    property with a fresh property-identity scoped to its class-identity.  The
    op-log is then replayed in order, maintaining a live ``current name ->
    identity`` map for classes (and, per class-identity, for its properties):

    * **create** (``create_class``/``create_subclass``/``create_property``) —
      MINT a brand-new identity (no baseline name) bound to that name, severing
      the name from any prior occupant.  A recycled name therefore binds to a
      *different* identity than the original — so a real rename of the original
      is never stolen, and the net-new entity is reported *added*, not renamed.
    * **rename** (``update_class`` ``new_name`` / ``rename_property``) — REBIND
      the existing identity's current name (never mint).
    * **delete** (``delete_class``/``delete_property``) — END the identity
      (drop it from the live map; its final name becomes ``None``).

    Cascade self-calls are absent from the op-log (recorded only at the
    outermost public boundary, ADR 0002), so the cascade's per-subclass property
    creations are invisible here and correctly fall to the diff's "added" path.

    :param baseline: The baseline schema dict (``Schema._baseline``).
    :param change_log: The ``Schema._change_log`` op-log entries.
    :returns: A :class:`RenameMap` carrying the baseline<->current name maps and
        the per-op-log-position call-time-name resolutions.
    """
    # Identity is an opaque monotonic int.  Per-identity records:
    #   baseline_name: the entity's name at baseline, or None if born later.
    #   final_name: its current live name, or None once deleted.
    next_id = 0
    class_baseline_name: dict[int, Optional[str]] = {}
    class_final_name: dict[int, Optional[str]] = {}
    # Live "current class name -> class identity"; only currently-alive classes.
    class_live: dict[str, int] = {}

    # Property identities are scoped by their owning CLASS IDENTITY, so a class
    # rename (which never changes the class identity) leaves them untouched.
    prop_baseline_name: dict[int, Optional[str]] = {}
    prop_final_name: dict[int, Optional[str]] = {}
    # (class_id) -> {current prop name -> prop identity} for alive properties.
    prop_live: dict[int, dict[str, int]] = {}
    # prop identity -> owning class identity, for EVERY property (alive or
    # deleted), so a deleted property's baseline scope key can still be derived.
    prop_owner: dict[int, int] = {}

    def mint_class(name: str, baseline_name: Optional[str]) -> int:
        nonlocal next_id
        cid = next_id
        next_id += 1
        class_baseline_name[cid] = baseline_name
        class_final_name[cid] = name
        class_live[name] = cid
        prop_live[cid] = {}
        return cid

    def mint_prop(class_id: int, name: str, baseline_name: Optional[str]) -> int:
        nonlocal next_id
        pid = next_id
        next_id += 1
        prop_baseline_name[pid] = baseline_name
        prop_final_name[pid] = name
        prop_owner[pid] = class_id
        prop_live[class_id][name] = pid
        return pid

    # ---- Seed from baseline ------------------------------------------------
    for cls in baseline.get("classes", []):
        cname = cls.get("name")
        if cname is None:
            continue
        cid = mint_class(cname, baseline_name=cname)
        for prop in cls.get("properties", []):
            pname = prop.get("name")
            if pname is None:
                continue
            mint_prop(cid, pname, baseline_name=pname)

    # ---- Replay the op-log, capturing per-position resolutions -------------
    entry_class_resolution: list[Optional[str]] = []
    entry_prop_resolution: list[Optional[tuple[str, str]]] = []

    for entry in change_log:
        op = entry.get("op")
        args = entry.get("args", {})
        call_class = args.get("class_name")
        call_prop = args.get("prop_name")

        # Capture the identity each call-time name refers to AT THIS POSITION,
        # BEFORE applying any rename/creation this entry itself performs (a
        # call-time name is the name as it was when the call was made).  Record
        # the captured identity id; it is mapped to a final name post-replay.
        captured_cid = class_live.get(call_class) if call_class is not None else None
        captured_pid: Optional[int] = None
        if captured_cid is not None and call_prop is not None:
            captured_pid = prop_live.get(captured_cid, {}).get(call_prop)
        # rename_property carries old_prop_name rather than prop_name.
        if op == "rename_property" and captured_cid is not None:
            old_prop = args.get("old_prop_name")
            captured_pid = prop_live.get(captured_cid, {}).get(old_prop)
        entry_class_resolution.append(captured_cid)  # int now; -> name later
        entry_prop_resolution.append(captured_pid)   # int now; -> identity later

        # ---- Apply the entry's own identity mutation -----------------------
        if op in _CREATE_CLASS_OPS:
            new_name = args.get("class_name")
            if new_name is not None:
                mint_class(new_name, baseline_name=None)

        elif op == "create_property":
            cid = class_live.get(call_class) if call_class is not None else None
            if cid is not None and call_prop is not None:
                mint_prop(cid, call_prop, baseline_name=None)

        elif op == "update_class":
            new_name = args.get("new_name")
            if new_name and new_name != call_class and call_class is not None:
                cid = class_live.get(call_class)
                if cid is not None:
                    del class_live[call_class]
                    class_live[new_name] = cid
                    class_final_name[cid] = new_name

        elif op == "rename_property":
            new_prop = args.get("new_prop_name")
            old_prop = args.get("old_prop_name")
            cid = class_live.get(call_class) if call_class is not None else None
            if (
                cid is not None
                and new_prop
                and new_prop != old_prop
            ):
                submap = prop_live.get(cid, {})
                pid = submap.get(old_prop)
                if pid is not None:
                    del submap[old_prop]
                    submap[new_prop] = pid
                    prop_final_name[pid] = new_prop

        elif op == "delete_class":
            cid = class_live.pop(call_class, None) if call_class is not None else None
            if cid is not None:
                class_final_name[cid] = None

        elif op == "delete_property":
            cid = class_live.get(call_class) if call_class is not None else None
            if cid is not None and call_prop is not None:
                pid = prop_live.get(cid, {}).pop(call_prop, None)
                if pid is not None:
                    prop_final_name[pid] = None

    # ---- Derive baseline -> current correspondence -------------------------
    # A property's baseline scope key needs its owning class identity.  Track
    # ownership for EVERY property identity (alive or deleted), captured at mint
    # time below would be ideal; we rebuild it here from the recorded owners.
    # prop_owner is populated as identities are minted (see mint_prop), so it is
    # complete for deleted properties too.
    classes: dict[str, str] = {}
    class_fate: dict[str, Optional[str]] = {}
    for cid, base_name in class_baseline_name.items():
        if base_name is None:
            continue  # born after baseline — not a baseline identity
        final = class_final_name[cid]
        class_fate[base_name] = final
        if final is not None and final != base_name:
            classes[base_name] = final

    properties: dict[tuple[str, str], str] = {}
    prop_fate: dict[tuple[str, str], Optional[str]] = {}
    for pid, base_name in prop_baseline_name.items():
        if base_name is None:
            continue  # born after baseline
        owner_cid = prop_owner.get(pid)
        if owner_cid is None:
            continue
        owner_base = class_baseline_name.get(owner_cid)
        if owner_base is None:
            continue
        final = prop_final_name[pid]
        prop_fate[(owner_base, base_name)] = final
        if final is not None and final != base_name:
            properties[(owner_base, base_name)] = final

    # ---- Map captured per-position identities to final current names -------
    resolved_class: list[Optional[str]] = [
        None if cid is None else class_final_name.get(cid)
        for cid in entry_class_resolution
    ]
    resolved_prop: list[Optional[tuple[str, str]]] = []
    for pid in entry_prop_resolution:
        if pid is None:
            resolved_prop.append(None)
            continue
        final_prop = prop_final_name.get(pid)
        owner_cid = prop_owner.get(pid)
        final_cls = class_final_name.get(owner_cid) if owner_cid is not None else None
        if final_prop is None or final_cls is None:
            resolved_prop.append(None)
        else:
            resolved_prop.append((final_cls, final_prop))

    return RenameMap(
        classes=classes,
        properties=properties,
        entry_class_resolution=resolved_class,
        entry_prop_resolution=resolved_prop,
        class_fate=class_fate,
        prop_fate=prop_fate,
    )


def _normalise_description(desc: Any) -> str:
    """Return the plain-text rendering of a description value.

    Descriptions are stored as ``{"en": "...", "@none": "..."}`` dicts.
    Plain strings are passed through unchanged; ``None`` maps to ``""``.

    :param desc: Raw description value from a schema dict.
    :returns: Plain text string.
    """
    if desc is None:
        return ""
    if isinstance(desc, dict):
        return desc.get("@none", desc.get("en", ""))
    return str(desc)


def _diff_property_fields(b_p: dict, c_p: dict) -> list[dict]:
    """Compute the per-field changes between two property definitions.

    Only genuinely changed fields are included.  Descriptions are normalised to
    plain text before comparison; date fields are never compared.

    :param b_p: Baseline property dict.
    :param c_p: Current property dict.
    :returns: List of ``{"field": str, "before": Any, "after": Any}`` entries.
    """
    changed_fields: list[dict] = []
    for f_name in _PROPERTY_FIELDS:
        b_val = b_p.get(f_name)
        c_val = c_p.get(f_name)
        if b_val != c_val:
            changed_fields.append({"field": f_name, "before": b_val, "after": c_val})

    b_desc = _normalise_description(b_p.get("description"))
    c_desc = _normalise_description(c_p.get("description"))
    if b_desc != c_desc:
        changed_fields.append({"field": "description", "before": b_desc, "after": c_desc})

    return changed_fields


def _diff_properties(
    b_props: list[dict],
    c_props: list[dict],
    class_name: str,
    rename_map: Optional[RenameMap] = None,
    current_class_name: Optional[str] = None,
) -> list[Change]:
    """Diff two ordered property lists for a single class, by identity.

    Properties are matched **by stable identity** through the *rename_map*'s
    per-baseline-entity fate (ADR 0003): each baseline property's fate is looked
    up by its baseline identity ``(class_name, b_name)`` — its final current
    name, or ``None`` if its identity was deleted during the session.  A deleted
    identity is ``removed`` and never matches a current property of the same
    name (which, if present, is a *recycled* identity reported ``added``).  A
    surviving property whose final name differs is one ``op="renamed"`` Change
    with field modifications merged in; a current property matched by no
    surviving baseline identity is ``added``.  An untracked rename (no op-log
    event) leaves the identity's name unchanged in the replay and so degrades to
    remove+add — exactly as ADR 0001 documents.

    A *reorder candidate* signal is attached as ``detail={"reorder_candidate":
    True}`` on a synthetic ``op="modified"`` Change with target
    ``"<ClassName>.__order__"`` when the property-name SET (after rename
    reconciliation) is unchanged but the ordered SEQUENCE differs.  Phase 5
    reconciles this against the op-log.

    :param b_props: Baseline property list.
    :param c_props: Current property list.
    :param class_name: Owning class's *baseline* name (used to scope rename
        lookups and to build ``target``).
    :param rename_map: Identity correspondence, or ``None`` for a pure name-keyed diff.
    :param current_class_name: The class's current name (for building ``target``
        on the current side).  Defaults to *class_name* when unchanged.
    :returns: List of ``Change`` instances.
    """
    if current_class_name is None:
        current_class_name = class_name

    changes: list[Change] = []

    # Guard against name-less property dicts (see _diff): skip, don't KeyError.
    b_map: dict[str, dict] = {p["name"]: p for p in b_props if "name" in p}
    c_map: dict[str, dict] = {p["name"]: p for p in c_props if "name" in p}

    matched_current: set[str] = set()
    # Map a baseline property name to the current name it was matched to (used
    # for the reorder-candidate sequence comparison below).
    base_to_current: dict[str, str] = {}

    # Baseline properties — match each to its current identity by FATE.
    for b_name, b_p in b_map.items():
        if rename_map is not None:
            # Sentinel distinguishes "no fate recorded" (pure name diff) from a
            # recorded None (identity deleted).  property_current_name's
            # name-defaulting fallback would conflate the two and resurrect a
            # deleted-then-recycled name — the V-B2 swallow.  Fate is explicit.
            fate = rename_map.prop_fate.get((class_name, b_name), b_name)
        else:
            fate = b_name

        if fate is None:
            # Identity deleted during the session — removed (and a current prop
            # of the same name, if any, is a recycled identity matched as added).
            changes.append(Change(
                target=f"{class_name}.{b_name}",
                kind="property",
                op="removed",
            ))
            continue

        cur_name = fate
        c_p = c_map.get(cur_name)
        if c_p is None:
            # Survivor whose final name is absent from current (e.g. removed via
            # untracked to_dict edit, or renamed-away by an untracked edit).
            changes.append(Change(
                target=f"{class_name}.{b_name}",
                kind="property",
                op="removed",
            ))
            continue

        matched_current.add(cur_name)
        base_to_current[b_name] = cur_name
        changed_fields = _diff_property_fields(b_p, c_p)

        if cur_name != b_name:
            # Renamed (possibly also modified) — ONE combined record.
            changes.append(Change(
                target=f"{current_class_name}.{cur_name}",
                kind="property",
                op="renamed",
                from_=b_name,
                to=cur_name,
                fields=changed_fields or None,
            ))
        elif changed_fields:
            changes.append(Change(
                target=f"{current_class_name}.{cur_name}",
                kind="property",
                op="modified",
                fields=changed_fields,
            ))

    # Current properties matched by no baseline identity — added.
    for c_name in c_map:
        if c_name not in matched_current:
            changes.append(Change(
                target=f"{current_class_name}.{c_name}",
                kind="property",
                op="added",
            ))

    # Reorder candidate: the reconciled identity SET is unchanged but the
    # ordered SEQUENCE of the common properties differs.  Compare baseline
    # order (by baseline name) against current order (by matched current name).
    matched_base_names = set(base_to_current)
    if matched_base_names == set(b_map) and len(matched_current) == len(c_map):
        common_b_order = [base_to_current[p["name"]] for p in b_props if p["name"] in base_to_current]
        common_c_order = [p["name"] for p in c_props if p["name"] in matched_current]
        if common_b_order != common_c_order:
            changes.append(Change(
                target=f"{current_class_name}.__order__",
                kind="property",
                op="modified",
                detail={"reorder_candidate": True, "before_order": common_b_order, "after_order": common_c_order},
            ))

    return changes


def _diff_class_fields(b_cls: dict, c_cls: dict) -> list[dict]:
    """Compute per-field changes between two class definitions.

    Only fields that actually differ are included.  Date fields are excluded.
    Descriptions are normalised to plain text before comparison.

    :param b_cls: Baseline class dict.
    :param c_cls: Current class dict.
    :returns: List of ``{"field": str, "before": Any, "after": Any}`` entries.
    """
    changed: list[dict] = []

    for f_name in _CLASS_FIELDS:
        b_val = b_cls.get(f_name)
        c_val = c_cls.get(f_name)
        if b_val != c_val:
            changed.append({"field": f_name, "before": b_val, "after": c_val})

    # Description normalised
    b_desc = _normalise_description(b_cls.get("description"))
    c_desc = _normalise_description(c_cls.get("description"))
    if b_desc != c_desc:
        changed.append({"field": "description", "before": b_desc, "after": c_desc})

    return changed


def _diff(
    baseline: dict,
    current: dict,
    rename_map: Optional[RenameMap] = None,
) -> list[Change]:
    """Compute the net-effect structural delta between two schema dicts.

    This is the authoritative source of structural truth.  It never touches
    ``createdDate`` or ``lastModifiedDate``.  Descriptions are normalised to
    plain text.  No-op sequences (create-then-delete, modify-then-revert)
    produce no entry.

    When a *rename_map* is supplied, classes and properties are matched **by
    canonical/baseline identity** rather than by current name (ADR 0003): a
    baseline class whose identity maps to a differently-named current class is
    reported as a single ``op="renamed"`` Change (``from_``/``to``) with any
    field-level modifications merged into that same record, and its properties
    are diffed scoped to the class's baseline identity.  A name that maps back
    to itself carries no rename label; an untracked rename (no op-log event)
    has no map entry and degrades to remove+add.

    :param baseline: Deep-copied baseline schema dict (from ``Schema._baseline``).
    :param current: Live current schema dict (from ``Schema._schema``).
    :param rename_map: Identity correspondence (from ``_replay_identities``), or
        ``None`` for a pure name-keyed diff.
    :returns: List of :class:`Change` instances in traversal order.
    """
    changes: list[Change] = []

    # ------------------------------------------------------------------
    # 1. Top-level metadata — compare 'name'; exclude date fields.
    # ------------------------------------------------------------------
    b_name = baseline.get("name", "")
    c_name = current.get("name", "")
    if b_name != c_name:
        changes.append(Change(
            target="schema.name",
            kind="metadata",
            op="modified",
            from_=b_name,
            to=c_name,
        ))

    # Compare other reportable top-level metadata keys.  Date fields and
    # server-internal/structural keys (guid, @context, type, ...) are excluded
    # via the allow-list so the report never claims a server-owned plumbing key
    # as a domain-model edit.
    def _reportable_meta(d: dict) -> dict:
        return {
            k: v for k, v in d.items()
            if k not in _DATE_FIELDS and k not in _METADATA_EXCLUDED_KEYS
        }

    b_extra = _reportable_meta(baseline)
    c_extra = _reportable_meta(current)
    all_extra_keys = set(b_extra) | set(c_extra)
    for key in sorted(all_extra_keys):
        b_val = b_extra.get(key)
        c_val = c_extra.get(key)
        if b_val != c_val:
            changes.append(Change(
                target=f"schema.{key}",
                kind="metadata",
                op="modified",
                from_=b_val,
                to=c_val,
            ))

    # ------------------------------------------------------------------
    # 2. Classes — name-keyed maps, O(n).
    # ------------------------------------------------------------------
    # Guard against name-less class dicts (API/legacy-shaped or untracked
    # to_dict edits): skip them rather than KeyError so change_report degrades
    # gracefully instead of crashing mid-computation.
    b_classes: dict[str, dict] = {
        cls["name"]: cls for cls in baseline.get("classes", []) if "name" in cls
    }
    c_classes: dict[str, dict] = {
        cls["name"]: cls for cls in current.get("classes", []) if "name" in cls
    }

    c_class_names = set(c_classes)
    matched_current: set[str] = set()

    # Baseline classes — match each to its current identity by FATE (the final
    # current name its identity survived to, or None if its identity was deleted
    # during the session).  Fate is recorded per baseline identity by the
    # event-sourced replay, so a name freed then recycled binds a *different*
    # identity: a deleted baseline class is reported removed and never matched
    # to a same-named recycled class (the V-B2 swallow is impossible here).
    for b_name, b_cls in b_classes.items():
        if rename_map is not None:
            fate = rename_map.class_fate.get(b_name, b_name)
        else:
            fate = b_name

        if fate is None:
            # Identity deleted — removed (a same-named current class is a
            # recycled identity, matched as added in the unmatched-current pass).
            changes.append(Change(
                target=b_name,
                kind="class",
                op="removed",
            ))
            continue

        cur_name = fate
        c_cls = c_classes.get(cur_name)

        if c_cls is None:
            # Survivor whose final name is absent from current (untracked edit).
            changes.append(Change(
                target=b_name,
                kind="class",
                op="removed",
            ))
            continue

        matched_current.add(cur_name)
        class_field_changes = _diff_class_fields(b_cls, c_cls)

        if cur_name != b_name:
            # Renamed (possibly also modified) — ONE combined record.
            changes.append(Change(
                target=cur_name,
                kind="class",
                op="renamed",
                from_=b_name,
                to=cur_name,
                fields=class_field_changes or None,
            ))
        elif class_field_changes:
            changes.append(Change(
                target=cur_name,
                kind="class",
                op="modified",
                fields=class_field_changes,
            ))

        prop_changes = _diff_properties(
            b_cls.get("properties", []),
            c_cls.get("properties", []),
            class_name=b_name,
            rename_map=rename_map,
            current_class_name=cur_name,
        )
        changes.extend(prop_changes)

    # Current classes matched by no baseline identity — added.
    for c_name in c_class_names:
        if c_name not in matched_current:
            changes.append(Change(
                target=c_name,
                kind="class",
                op="added",
            ))

    return changes


# ---------------------------------------------------------------------------
# Semantic annotation layer — Phase 5
# ---------------------------------------------------------------------------
#
# _annotate(changes, change_log, baseline, current, rename_map=None)
#       -> list[Change]
#
# Upgrades raw structural Changes (from _diff) into single semantic entries
# wherever the op-log records that the user expressed a higher-level intent.
# This layer never invents or contradicts structural truth (ADR 0001): it only
# RELABELS or COLLAPSES Changes the diff already produced, keyed off the single
# op-log entry recorded at the outermost public-call boundary (ADR 0002).  When
# the op-log carries no matching entry the structural Changes are passed through
# untouched (graceful degradation — ADR 0001), so an untracked reorder, a
# to_dict() edit, etc. surface exactly as the diff reported them.
#
# Op-log class/property names are the entity names *as they were at call time*;
# the rename_map (ADR 0003) resolves them to current names so annotation matches
# the identity-aware Change targets emitted by _diff.
#
# Final ordering is preserved: each annotation replaces/edits Changes in place
# (no reshuffle), so Phase 6's render sees a stable, diff-ordered sequence.
# ---------------------------------------------------------------------------


def _annotate(
    changes: list[Change],
    change_log: list[dict],
    baseline: dict,
    current: dict,
    rename_map: Optional[RenameMap] = None,
) -> list[Change]:
    """Layer semantic intent from the op-log onto identity-aware structural Changes.

    Runs *after* ``_diff`` + rename folding.  Walks the op-log and, for the
    intent-bearing ops, upgrades the corresponding raw Changes into a single
    semantic record.  Changes with no matching op-log entry are returned
    unchanged (ADR 0001 graceful degradation).

    The following upgrades are applied:

    * ``create_subclass`` — the diff reports the new subclass as a single
      ``op="added"`` class Change (an added class does not expand into
      per-property adds).  It is replaced by one ``op="subclass_created"``
      Change with ``detail={"parent": <parent>, "inherited": N}`` where *N* is
      the number of properties on the newly-created subclass (its full inherited
      property set, mirroring the parent).

    * ``create_property`` / ``update_property`` with ``apply_to_subclasses`` —
      the op-log records the ``apply_to_subclasses`` intent flag.  When the flag
      is set and the parent has at least one current subclass, the parent
      property Change is *referenced* from every current subclass via
      ``detail={"applied_to_subclasses": [<subclass current name>, ...]}``,
      derived from issued INTENT (the op-log + the current subclass set), NOT
      from a per-subclass diff effect — so the annotation appears even when a
      subclass already held the value and produced no structural Change (ADR
      0001/0002: the op-log is the authoritative intent source).  The per-subclass
      structural Changes (where they exist) are kept as ordinary records.  No
      annotation is added when the flag is unset or the parent has no subclasses.

    * ``assign_label_property`` — the diff reports a class ``labelProperty``
      field flip plus the designated property's ``isOptional``/``isLangString``
      flips.  The two are fused: the class Change's ``labelProperty`` field entry
      is dropped (if it was the only changed field the whole class Change is
      removed), and the property Change gains
      ``detail={"label_property": <prop current name>}`` marking it as the
      coherent label-property designation rather than two unrelated flips.

    * ``assign_property_orders`` — driven directly from the op-log entry (the
      authoritative intent, ADR 0001).  For each class in the entry the relative
      order of the properties present in BOTH baseline and current is compared;
      if it differs, an ``op="reordered"`` Change (``target=<Class current
      name>``, ``detail={"order": [<current order>]}``) is emitted, INDEPENDENT
      of any concurrent property add/remove (which suppresses the diff's
      exact-set ``__order__`` candidate).  Any structural ``__order__`` reorder
      candidate for the class is consumed: replaced by the ``reordered`` Change
      on a genuine reorder, or dropped when the surviving properties are
      unchanged in order (so the internal sentinel never surfaces on a tracked
      no-op).  An untracked reorder (no op-log entry) is left untouched —
      documented degradation (ADR 0001).

    * ``delete_class(cascade_to_subclasses=True)`` — the stripped ``subClassOf``
      on former subclasses is genuine structural truth and is left exactly as
      the diff reported it (an ``op="modified"`` class Change whose ``fields``
      carries ``subClassOf: <parent> -> None``).  Pinned by tests as documented
      behaviour.

    :param changes: The identity-aware Changes from ``_diff``.
    :param change_log: The ``Schema._change_log`` op-log entries.
    :param baseline: The baseline schema dict (for parent/inheritance lookups).
    :param current: The current schema dict (for subclass / property lookups).
    :param rename_map: Identity correspondence (from ``_replay_identities``), or
        ``None``.  Used to resolve op-log call-time names to current names.
    :returns: A new list of annotated Changes in the original diff order.
    """
    if rename_map is None:
        rename_map = RenameMap(classes={}, properties={})

    # Index current classes by name for inheritance / subclass lookups.
    # Name-less dicts (untracked to_dict edits) are skipped, not KeyError'd.
    current_classes: dict[str, dict] = {
        cls["name"]: cls for cls in current.get("classes", []) if "name" in cls
    }
    baseline_classes: dict[str, dict] = {
        cls["name"]: cls for cls in baseline.get("classes", []) if "name" in cls
    }

    def _current_class_name(position: int, call_time_name: str) -> str:
        """Resolve an op-log entry's call-time class name to its current name.

        Resolution is keyed by op-log *position* (FIX VR-B2), so an op issued
        under a name later freed and recycled by a different entity resolves to
        the entity that bore that name at issue time, not to whichever entity
        last held it.  Intermediate names in a rename chain (FIX V-B1) resolve
        through the same per-position mechanism.
        """
        return rename_map.resolve_class_at(position, call_time_name)

    def _current_prop_identity(
        position: int, call_time_class: str, call_time_prop: str
    ) -> tuple[str, str]:
        """Resolve an op-log entry's call-time ``(class, prop)`` to current names."""
        return rename_map.resolve_property_at(
            position, call_time_class, call_time_prop
        )

    # Index current subclasses by parent ONCE (a single O(C) pass), and the set
    # of live class names ONCE, so the apply_to_subclasses path is O(1) per op
    # rather than re-scanning every class per op (no L*C — NFR).
    subclasses_by_parent: dict[str, list[str]] = {}
    live_class_names: set[str] = set()
    for cls in current.get("classes", []):
        cname = cls.get("name")
        if cname is None:
            continue
        live_class_names.add(cname)
        parent = cls.get("subClassOf")
        if parent is not None:
            subclasses_by_parent.setdefault(parent, []).append(cname)

    # Invert the class rename map ONCE (current name -> baseline name) for the
    # reorder path, an O(1) lookup instead of a per-op linear scan.
    current_to_baseline_class: dict[str, str] = {
        cur: base for base, cur in rename_map.classes.items()
    }

    # Whether ANY class was renamed this session.  When none were, recorded
    # op-time subclass names are already current — resolution is a no-op and the
    # O(L*C) eager resolution table the prior model built is unnecessary (NFR).
    any_class_rename = bool(rename_map.classes)

    # Memoise resolution across ops (FIX round-4 B2 — report-path quadratic).
    # Cascade ops on the SAME parent record the SAME transitive subclass set, so
    # without a memo L such ops each re-resolve the same O(C) list ⇒ O(L*C).  The
    # memo keys the resolved list by the recorded tuple, so identical sets resolve
    # ONCE and share the result (and the same list object — also bounding memory):
    # total resolution work collapses to O(distinct_sets * C + L) ≈ O(C + L).
    _resolve_memo: dict[tuple[str, ...], list[str]] = {}

    def _resolve_applied(recorded: list[str]) -> list[str]:
        """Resolve an op's recorded op-time subclass names to current, surviving names.

        Done LAZILY here, only for the handful of ops that reach the cascade
        branch, and MEMOISED across ops sharing the same recorded set so repeated
        cascade ops on one parent do not each pay an O(C) re-resolution (B2).

        Each recorded name is an op-time current class name.  When no class was
        renamed, recorded names are already current.  Otherwise a recorded name
        that was a baseline class resolves through ``class_current_name`` (its
        baseline identity's final name); a mid-session-created subclass has no
        baseline identity and resolves to its op-time name.  Finally the set is
        intersected with the classes that still exist (net-effect consistency:
        the report must not claim a property "applied to" a class another record
        marks ``removed``).
        """
        key = tuple(recorded)
        cached = _resolve_memo.get(key)
        if cached is not None:
            return cached
        out: list[str] = []
        seen: set[str] = set()
        for name in recorded:
            cur = rename_map.class_current_name(name) if any_class_rename else name
            if cur in live_class_names and cur not in seen:
                seen.add(cur)
                out.append(cur)
        _resolve_memo[key] = out
        return out

    # Work on a shallow copy of the list; individual Changes are frozen so any
    # upgrade produces a replacement instance.
    result: list[Change] = list(changes)

    # Classes that already carry a net "reordered" Change, so multiple
    # assign_property_orders ops on one class collapse to a single net reorder
    # (FIX VR-B4 — net-effect-collapse; never emit two reordered for one class).
    reordered_classes: set[str] = set()

    # ------------------------------------------------------------------
    # O(1) lookup indices, built ONCE (FIX V-B5 — eliminate the per-op linear
    # _find_index scan that made _annotate O(L*D), worst case O(L*S*D)).
    #
    # by_target maps a Change.target to the list of result-list indices whose
    # Change currently has that target.  When a Change is replaced in place its
    # target may change (e.g. an "__order__" candidate becomes a class target),
    # so _replace() re-points the index map and tombstones remove their index.
    # All branches below look up candidates through these maps in O(1)+O(matches)
    # rather than scanning the whole list, giving overall O(L + D).
    # ------------------------------------------------------------------
    by_target: dict[str, list[int]] = {}
    for i, ch in enumerate(result):
        by_target.setdefault(ch.target, []).append(i)

    def _index_for(target: str, predicate) -> Optional[int]:
        """First live index whose Change has *target* and satisfies *predicate*."""
        for i in by_target.get(target, ()):
            ch = result[i]
            if ch is not None and predicate(ch):
                return i
        return None

    def _replace(i: int, new_ch: Optional[Change]) -> None:
        """Replace result[i], keeping the by_target index consistent."""
        old = result[i]
        if old is not None and (new_ch is None or new_ch.target != old.target):
            bucket = by_target.get(old.target)
            if bucket is not None:
                try:
                    bucket.remove(i)
                except ValueError:
                    pass
        if new_ch is not None and (old is None or new_ch.target != old.target):
            by_target.setdefault(new_ch.target, []).append(i)
        result[i] = new_ch

    for position, entry in enumerate(change_log):
        op = entry.get("op")
        args = entry.get("args", {})

        # ---- create_subclass: collapse the class-add into subclass_created ---
        if op == "create_subclass":
            sub_current = _current_class_name(position, args.get("class_name", ""))
            parent_call = args.get("parent_class_name", "")
            idx = _index_for(
                sub_current,
                lambda ch: ch.kind == "class" and ch.op == "added",
            )
            if idx is None:
                # No matching add (e.g. the subclass was later deleted, net no-op)
                # — nothing to annotate; structural truth already correct.
                continue
            sub_cls = current_classes.get(sub_current, {})
            current_prop_names = [
                p["name"] for p in sub_cls.get("properties", []) if "name" in p
            ]
            recorded_inherited = args.get("inherited_properties")
            if recorded_inherited is None:
                # Legacy/absent op-log shape — degrade to the live property count
                # (ADR 0001 graceful degradation; no post-creation surfacing
                # possible without the captured set).
                inherited = len(current_prop_names)
                post_creation: list[str] = []
            else:
                # ``inherited`` reflects the properties inherited from the parent
                # AT CREATION TIME that still survive (round-4 B2): a property the
                # user deleted from the subclass afterwards is no longer counted,
                # and one ADDED afterwards is NOT absorbed into the count.
                inherited_set = set(recorded_inherited)
                inherited = sum(
                    1 for name in current_prop_names if name in inherited_set
                )
                # Any current property NOT in the inherited-at-creation set was
                # added to the subclass after create_subclass; surface each as its
                # own ``added`` record rather than silently inflating the count.
                post_creation = [
                    name for name in current_prop_names
                    if name not in inherited_set
                ]
            _replace(idx, Change(
                target=sub_current,
                kind="class",
                op="subclass_created",
                detail={"parent": parent_call, "inherited": inherited},
            ))
            for prop_name in post_creation:
                added_target = f"{sub_current}.{prop_name}"
                new_idx = len(result)
                result.append(Change(
                    target=added_target,
                    kind="property",
                    op="added",
                ))
                by_target.setdefault(added_target, []).append(new_idx)

        # ---- apply_to_subclasses: reference subclasses from the parent op -----
        elif op in ("create_property", "update_property"):
            # The annotation is gated on the recorded INTENT flag, not on a
            # diff effect (FIX V-B4): a create/update_property that did NOT
            # request apply_to_subclasses must never carry the detail.
            if not args.get("apply_to_subclasses"):
                continue
            parent_call = args.get("class_name", "")
            prop_call = args.get("prop_name", "")
            parent_current, prop_current = _current_prop_identity(
                position, parent_call, prop_call
            )
            # FIX VR-B3: derive applied_to_subclasses from the set the op
            # actually applied to AT CALL TIME (recorded in the op-log entry),
            # mapped through aliases to current names — NOT from the report-time
            # subclass set (which over-claims subclasses created after the op)
            # and NOT from a per-subclass diff effect (which under-claims).  Each
            # recorded subclass name is a call-time name resolved per position.
            recorded = args.get("applied_subclasses")
            if recorded is None:
                # Legacy/absent op-log shape — degrade to the live subclass set
                # (ADR 0001 graceful degradation when the log lacks the detail).
                applied = list(subclasses_by_parent.get(parent_current, []))
            else:
                # Resolve the recorded op-time subclass names to their final,
                # still-surviving current names — lazily, O(applied) per op.
                applied = _resolve_applied(recorded)
            if not applied:
                # The call applied to nothing — leave the parent unannotated.
                continue
            parent_target = f"{parent_current}.{prop_current}"
            pidx = _index_for(
                parent_target,
                lambda ch: ch.kind == "property" and ch.op in ("added", "modified"),
            )
            if pidx is None:
                # FIX VR-B3(a): the parent property itself produced no structural
                # Change (e.g. the parent already held the value), but the op was
                # genuinely issued with apply_to_subclasses intent.  The op-log
                # HAS the entry, so per ADR 0001 we surface the intent rather than
                # dropping it: synthesise a parent property Change carrying the
                # applied_to_subclasses annotation (no field changes — it records
                # what the op DID, not a structural field flip the diff didn't see).
                new_idx = len(result)
                result.append(Change(
                    target=parent_target,
                    kind="property",
                    op="modified",
                    detail={"applied_to_subclasses": applied},
                ))
                by_target.setdefault(parent_target, []).append(new_idx)
                continue
            parent_ch = result[pidx]
            new_detail = dict(parent_ch.detail or {})
            new_detail["applied_to_subclasses"] = applied
            _replace(pidx, Change(
                target=parent_ch.target,
                kind=parent_ch.kind,
                op=parent_ch.op,
                from_=parent_ch.from_,
                to=parent_ch.to,
                fields=parent_ch.fields,
                detail=new_detail,
            ))

        # ---- assign_label_property: fuse class flip + property flips ---------
        elif op == "assign_label_property":
            cls_call = args.get("class_name", "")
            prop_call = args.get("prop_name", "")
            cls_current, prop_current = _current_prop_identity(
                position, cls_call, prop_call
            )
            prop_target = f"{cls_current}.{prop_current}"

            # Annotate the property Change with the label-property designation.
            pidx = _index_for(
                prop_target,
                lambda ch: ch.kind == "property" and ch.op in ("modified", "renamed"),
            )
            if pidx is not None:
                prop_ch = result[pidx]
                new_detail = dict(prop_ch.detail or {})
                new_detail["label_property"] = prop_current
                _replace(pidx, Change(
                    target=prop_ch.target,
                    kind=prop_ch.kind,
                    op=prop_ch.op,
                    from_=prop_ch.from_,
                    to=prop_ch.to,
                    fields=prop_ch.fields,
                    detail=new_detail,
                ))

            # Drop the now-redundant bare labelProperty flip from the class
            # Change.  If labelProperty was its only changed field, remove the
            # whole class Change; otherwise keep the other field changes.
            cidx = _index_for(
                cls_current,
                lambda ch: ch.kind == "class" and ch.op in ("modified", "renamed"),
            )
            if cidx is not None:
                cls_ch = result[cidx]
                remaining = [
                    f for f in (cls_ch.fields or [])
                    if f.get("field") != "labelProperty"
                ]
                if remaining:
                    _replace(cidx, Change(
                        target=cls_ch.target,
                        kind=cls_ch.kind,
                        op=cls_ch.op,
                        from_=cls_ch.from_,
                        to=cls_ch.to,
                        fields=remaining,
                        detail=cls_ch.detail,
                    ))
                elif cls_ch.op == "modified":
                    # The labelProperty flip was the only thing this modified
                    # class Change carried — tombstone it.  A renamed class
                    # Change is kept (the rename is independent structural truth).
                    _replace(cidx, None)

        # ---- assign_property_orders: drive reorder from issued intent --------
        elif op == "assign_property_orders":
            property_orders = args.get("property_orders", {}) or {}
            for cls_call in property_orders:
                cls_current = _current_class_name(position, cls_call)
                order_target = f"{cls_current}.__order__"

                # Consume any structural reorder candidate for this class (it is
                # an internal sentinel that must never surface on its own).
                cand_idx = _index_for(
                    order_target,
                    lambda ch: ch.detail is not None
                    and ch.detail.get("reorder_candidate") is True,
                )

                # FIX V-B3 / VR-B4: compute the NET reorder once per class from
                # the live current order vs the baseline order of the properties
                # present in BOTH, INDEPENDENT of any concurrent add/remove (which
                # suppresses the diff's exact-set candidate) and INDEPENDENT of how
                # many assign_property_orders ops touched the class.  Net-effect
                # collapse (ADR 0001): the current order already reflects the net
                # of every reorder op, so a single reordered Change describes them
                # all; we never emit a second for a class already covered.
                cur_cls = current_classes.get(cls_current, {})
                cur_order = [p["name"] for p in cur_cls.get("properties", [])]

                # Baseline order, mapped to current names, scoped to this class's
                # baseline identity (O(1) lookup via the once-inverted map).
                base_class_name = current_to_baseline_class.get(
                    cls_current, cls_current
                )
                base_cls = baseline_classes.get(base_class_name, {})
                base_order_current = [
                    rename_map.property_current_name(base_class_name, p["name"])
                    for p in base_cls.get("properties", [])
                ]

                common = set(cur_order) & set(base_order_current)
                cur_common = [n for n in cur_order if n in common]
                base_common = [n for n in base_order_current if n in common]
                reordered = len(common) >= 2 and cur_common != base_common

                if not reordered:
                    # No net reorder of common props — drop any sentinel and emit
                    # nothing (a reorder op whose net effect equals baseline).
                    if cand_idx is not None:
                        _replace(cand_idx, None)
                    continue

                if cls_current in reordered_classes:
                    # Net reorder already recorded for this class by an earlier
                    # op — collapse to one Change.  Consume any lingering sentinel
                    # but do not append a duplicate (FIX VR-B4).
                    if cand_idx is not None:
                        _replace(cand_idx, None)
                    continue

                reordered_classes.add(cls_current)
                if cand_idx is not None:
                    _replace(cand_idx, Change(
                        target=cls_current,
                        kind="class",
                        op="reordered",
                        detail={"order": cur_order},
                    ))
                else:
                    # No candidate emitted by the diff (the property set also
                    # changed) — surface the reorder from intent.  Append a new
                    # Change and register it in the index map.
                    new_idx = len(result)
                    result.append(Change(
                        target=cls_current,
                        kind="class",
                        op="reordered",
                        detail={"order": cur_order},
                    ))
                    by_target.setdefault(cls_current, []).append(new_idx)

    return [ch for ch in result if ch is not None]


# ---------------------------------------------------------------------------
# Rendering layer — Phase 6
# ---------------------------------------------------------------------------
#
# _sort_changes(changes) -> list[Change]
#     Deterministic ordering: (kind_rank, class_name, op_rank, target)
#
# _render_text(changes) -> str
#     Plain-text changelog with header and per-class grouping.
#
# _render_records(changes) -> list[dict]
#     Structured record dicts per the contract table; absent keys omitted.
# ---------------------------------------------------------------------------

#: kind_rank: metadata(0) < class(1) < property(2)
_KIND_RANK: dict[str, int] = {"metadata": 0, "class": 1, "property": 2}

#: op_rank: a fixed stable order shared by both renderers
_OP_RANK: dict[str, int] = {
    "added": 0,
    "subclass_created": 1,
    "renamed": 2,
    "modified": 3,
    "reordered": 4,
    "removed": 5,
}


def _change_class_name(ch: "Change") -> str:
    """Derive the owning class name from a Change's target.

    For ``"Class.prop"`` targets this returns ``"Class"``.  For bare class or
    metadata targets the target itself is returned so the sort stays total.

    :param ch: The change to inspect.
    :returns: The class-level sort key string.
    """
    if ch.kind == "property":
        # Target is always "ClassName.propName" for properties.
        return ch.target.split(".")[0]
    return ch.target


def _sort_changes(changes: list["Change"]) -> list["Change"]:
    """Return a copy of *changes* sorted into a deterministic rendering order.

    Sort key: ``(kind_rank, class_name, op_rank, target)``

    * ``kind_rank``: metadata first (0), then classes (1), then properties (2).
    * ``class_name``: alphabetical — groups property changes under their owning
      class and keeps class-level changes immediately before their properties.
    * ``op_rank``: stable across ops — added < subclass_created < renamed <
      modified < reordered < removed.
    * ``target``: alphabetical tie-break within the same class and op.
    * a final fully-serialised tie-break (``from_``/``to``/``fields``/``detail``)
      so the order is a TOTAL order: two Changes sharing all four primary
      components (e.g. a recycled name's renamed + removed share kind/class/op
      rank only partially, but a defensive total order must not fall back to
      insertion order) still sort deterministically, independent of dict
      insertion order (ADR 0001 determinism requirement).

    Implementation note (performance): the json tie-break is expensive
    (``json.dumps`` of ``fields``/``detail``, where a cascade op's ``detail``
    carries an O(C) ``applied_to_subclasses`` list).  Under an
    ``apply_to_subclasses`` fan-out there are O(L*C) changes, so serialising the
    tie-break key for *every* change is wasteful — the primary 4-tuple already
    disambiguates the overwhelming majority of changes.  This sort is therefore
    two-phase: sort on the primary tuple first, then serialise the json tie-break
    *only* within runs that tie on all four primary components.  This yields a
    total order byte-identical to a single composite key (within each
    primary-equal run the json key is the sole remaining discriminator, and the
    primary sort below is stable), while removing the json serialisation from the
    common, collision-free path.

    :param changes: The annotated list of :class:`Change` instances.
    :returns: A new list sorted into rendering order; *changes* is not mutated.
    """

    def _primary_key(ch: "Change") -> tuple[int, str, int, str]:
        return (
            _KIND_RANK.get(ch.kind, 99),
            _change_class_name(ch),
            _OP_RANK.get(ch.op, 99),
            ch.target,
        )

    def _tiebreak_key(ch: "Change") -> str:
        # Fully-deterministic tie-break: serialise the remaining discriminating
        # fields so the comparator never falls back to the input list order.
        # json with sorted keys is stable across runs (ADR 0001 determinism).
        return json.dumps(
            [ch.from_, ch.to, ch.fields, ch.detail],
            sort_keys=True,
            default=str,
        )

    # Phase 1: sort on the cheap primary tuple (stable).
    primary_sorted = sorted(changes, key=_primary_key)

    # Phase 2: only within runs that tie on the full primary tuple, refine by the
    # expensive json tie-break.  Runs of size 1 (the common case) skip json
    # entirely.
    result: list["Change"] = []
    for _, group in groupby(primary_sorted, key=_primary_key):
        run = list(group)
        if len(run) > 1:
            run.sort(key=_tiebreak_key)
        result.extend(run)
    return result


def _bool_str(v: Any) -> str:
    """Render a boolean as lowercase ``'true'``/``'false'``; pass anything else through."""
    if v is True:
        return "true"
    if v is False:
        return "false"
    return str(v) if v is not None else "null"


def _render_text(changes: list["Change"]) -> str:
    """Render an ordered list of :class:`Change` instances as a plain-text changelog.

    Layout::

        Schema changes (N):
        + ClassName [new class]
        + ClassName [new subclass of Parent] (+M inherited)
        - ClassName [removed]
        ~ ClassName [modified]
          field: before -> after
        ~ ClassName [reordered]
          properties reordered: [p1, p2, ...]
        ~ ClassName [renamed from OldName]
          field: before -> after
          + newProp [added]
          ~ dose: dosage -> dose [renamed]; isOptional: true -> false
          - oldProp [removed]

    *N* counts only the top-level class/metadata entries (not individual
    property-level lines).

    :param changes: Ordered :class:`Change` instances (already sorted).
    :returns: Deterministic plain-text changelog string.
    """
    lines: list[str] = []

    # Group property changes by owning class for inline rendering under class blocks.
    # We'll iterate in sorted order and collect property lines as we encounter them.
    # We need to bucket property changes by class_name so they render inline.
    prop_changes_by_class: dict[str, list["Change"]] = {}
    for ch in changes:
        if ch.kind == "property":
            cls = ch.target.split(".")[0]
            prop_changes_by_class.setdefault(cls, []).append(ch)

    # Track which class names we've already emitted a class-level block for,
    # so we can attach orphan property changes (adds/removes/modifies on
    # unchanged classes) without a duplicate header.
    emitted_class_blocks: set[str] = set()

    # Count top-level entries for the header: metadata changes + class changes
    # (each class block is one top-level entry; property-only changed classes
    # also count as one entry).
    #
    # To stay predictable: one entry per (kind=="metadata") change plus one
    # entry per distinct owning class that has any change.
    classes_with_changes: set[str] = set()
    metadata_count = 0
    for ch in changes:
        if ch.kind == "metadata":
            metadata_count += 1
        elif ch.kind == "class":
            classes_with_changes.add(ch.target)
        elif ch.kind == "property":
            classes_with_changes.add(ch.target.split(".")[0])
    top_level_n = metadata_count + len(classes_with_changes)

    if top_level_n > 0:
        lines.append(f"Schema changes ({top_level_n}):")

    def _format_class_header(ch: "Change") -> str:
        """Build the single class-level header line for class Changes."""
        if ch.op == "added":
            return f"+ {ch.target} [new class]"
        if ch.op == "subclass_created":
            parent = (ch.detail or {}).get("parent", "?")
            inherited = (ch.detail or {}).get("inherited", 0)
            return f"+ {ch.target} [new subclass of {parent}] (+{inherited} inherited)"
        if ch.op == "removed":
            return f"- {ch.target} [removed]"
        if ch.op == "reordered":
            order = (ch.detail or {}).get("order", [])
            return f"~ {ch.target} [reordered]"
        if ch.op == "renamed":
            from_name = ch.from_ or "?"
            suffix = f" [renamed from {from_name}]"
            return f"~ {ch.target}{suffix}"
        # modified
        return f"~ {ch.target} [modified]"

    def _render_class_fields(ch: "Change") -> list[str]:
        """Indented field-change lines for a modified/renamed class Change."""
        field_lines = []
        for f in (ch.fields or []):
            before_v = _bool_str(f.get("before"))
            after_v = _bool_str(f.get("after"))
            field_lines.append(f"  {f['field']}: {before_v} -> {after_v}")
        return field_lines

    def _render_detail_lines(ch: "Change", indent: str) -> list[str]:
        """Indented continuation lines for an entry's semantic annotations.

        The default (``text``) output MUST surface **every** detail dimension
        the ``records`` format carries, or the two renderings describe different
        logical changes and the cross-format invariant policing them is blind to
        the gap.  The five published detail dimensions
        (``applied_to_subclasses``, ``label_property``, ``order``, ``parent``,
        ``inherited``) each render on their own continuation line.

        List/string values are JSON-encoded (not naively ``", "``-joined) so a
        value containing a delimiter (e.g. a class name with ``", "``) round-trips
        unambiguously and cannot fool the invariant's text-side parser.  The
        ``order``/``parent``/``inherited`` lines are continuation detail emitted
        *in addition to* the human-readable header summary (which keeps the
        familiar ``[new subclass of P] (+N inherited)`` / ``[reordered]`` shape),
        so the text contract gains content without losing its existing form.

        :param ch: The change whose detail to render.
        :param indent: Leading whitespace (``"  "`` class-level, ``"    "``
            property-level) so the line nests under its owning block.
        :returns: Zero or more annotation lines.
        """
        detail = ch.detail or {}
        out: list[str] = []
        applied = detail.get("applied_to_subclasses")
        if applied:
            out.append(
                f"{indent}applied to subclasses: {json.dumps(applied)}"
            )
        if detail.get("label_property") is not None:
            out.append(
                f"{indent}designated label property: "
                f"{json.dumps(detail['label_property'])}"
            )
        if detail.get("order") is not None:
            out.append(
                f"{indent}reorder sequence: {json.dumps(detail['order'])}"
            )
        if detail.get("parent") is not None:
            out.append(
                f"{indent}subclass of: {json.dumps(detail['parent'])}"
            )
        if detail.get("inherited") is not None:
            out.append(
                f"{indent}inherited count: {json.dumps(detail['inherited'])}"
            )
        return out

    def _render_prop_line(pch: "Change") -> str:
        """Single indented property-change line (without detail continuations)."""
        prop_name = pch.target.split(".", 1)[1] if "." in pch.target else pch.target
        if pch.op == "added":
            return f"  + {prop_name} [added]"
        if pch.op == "removed":
            return f"  - {prop_name} [removed]"
        if pch.op == "renamed":
            parts = [f"  ~ {prop_name}: {pch.from_} -> {pch.to} [renamed]"]
            for f in (pch.fields or []):
                before_v = _bool_str(f.get("before"))
                after_v = _bool_str(f.get("after"))
                parts.append(f"; {f['field']}: {before_v} -> {after_v}")
            return "".join(parts)
        # modified
        parts: list[str] = [f"  ~ {prop_name}"]
        field_parts = []
        for f in (pch.fields or []):
            before_v = _bool_str(f.get("before"))
            after_v = _bool_str(f.get("after"))
            field_parts.append(f"{f['field']}: {before_v} -> {after_v}")
        if field_parts:
            parts.append(": " + "; ".join(field_parts))
        return "".join(parts)

    def _emit_prop_change(pch: "Change") -> None:
        """Emit a property change line plus any annotation continuation lines.

        A ``modified`` property carrying neither field changes nor renderable
        detail is content-free noise (it would print ``~ prop`` asserting a
        change while showing none) and is skipped (B2).  The corresponding
        ``records`` entry is likewise contentless, so both formats agree.
        """
        detail_lines = _render_detail_lines(pch, indent="    ")
        if pch.op == "modified" and not pch.fields and not detail_lines:
            return
        lines.append(_render_prop_line(pch))
        lines.extend(detail_lines)

    def _emit_one_class_change(class_ch: "Change") -> None:
        """Emit the header line + any detail lines for a single class Change."""
        lines.append(_format_class_header(class_ch))
        if class_ch.op in ("modified", "renamed"):
            lines.extend(_render_class_fields(class_ch))
        elif class_ch.op == "reordered":
            order = (class_ch.detail or {}).get("order", [])
            lines.append(f"  properties reordered: {order}")
        lines.extend(_render_detail_lines(class_ch, indent="  "))

    # All class-level Changes for a class, in sorted order.  FIX VR-B1: a class
    # may legitimately carry MORE THAN ONE class-level Change (a recycled name
    # yields renamed + removed; a concurrent reorder yields modified/renamed +
    # reordered).  Every such Change must render — the renderer must not silently
    # drop a destructive `removed` or a `reordered` after emitting the first
    # block, or the default text output would lie while records stay correct.
    class_changes_by_class: dict[str, list["Change"]] = {}
    for ch in changes:
        if ch.kind == "class":
            class_changes_by_class.setdefault(ch.target, []).append(ch)

    def _emit_class_block(cls_name: str) -> None:
        """Emit ALL class-level Changes for *cls_name*, then its property lines."""
        emitted_class_blocks.add(cls_name)

        class_chs = class_changes_by_class.get(cls_name, [])
        if class_chs:
            for class_ch in class_chs:
                _emit_one_class_change(class_ch)
        else:
            # Class has only property-level changes — synthesise a ~ header.
            lines.append(f"~ {cls_name} [modified]")

        # Append property-level lines (already in sorted order from _sort_changes).
        for pch in prop_changes_by_class.get(cls_name, []):
            _emit_prop_change(pch)

    # Walk changes in sorted order; emit each class's full block on first
    # encounter (the block renders every class-level Change for that class).
    for ch in changes:
        if ch.kind == "metadata":
            from_v = _bool_str(ch.from_)
            to_v = _bool_str(ch.to)
            lines.append(f"~ {ch.target}: {from_v} -> {to_v}")
            continue

        if ch.kind == "class":
            cls_name = ch.target
        else:  # property
            cls_name = ch.target.split(".")[0]
        if cls_name not in emitted_class_blocks:
            _emit_class_block(cls_name)

    return "\n".join(lines)


def _render_records(changes: list["Change"]) -> list[dict]:
    """Render an ordered list of :class:`Change` instances as structured record dicts.

    Each dict follows the Data/Interface Contract key table:

    * ``target`` (always present): dotted path — ``"ClassName"`` or
      ``"ClassName.propName"`` (current name).
    * ``kind`` (always present): ``"class"``, ``"property"``, or ``"metadata"``.
    * ``op`` (always present): one of ``added``, ``removed``, ``modified``,
      ``renamed``, ``reordered``, ``subclass_created``.
    * ``from`` (renames only): previous name (mapped from :attr:`Change.from_`).
    * ``to`` (renames only): new name (mapped from :attr:`Change.to`).
    * ``fields`` (modified/renamed with field changes only): list of
      ``{"field": str, "before": Any, "after": Any}`` entries.
    * ``detail`` (compound/label/reorder only): the annotation dict.

    Keys are **omitted** (not ``None``) when they do not apply to a given entry,
    keeping records compact and stable.

    Records are emitted in the same order as :func:`_render_text` lines —
    identical to the sorted order supplied.

    :param changes: Ordered :class:`Change` instances (already sorted).
    :returns: Deterministic list of record dicts.
    """
    records: list[dict] = []
    for ch in changes:
        rec: dict = {
            "target": ch.target,
            "kind": ch.kind,
            "op": ch.op,
        }
        if ch.from_ is not None:
            rec["from"] = ch.from_
        if ch.to is not None:
            rec["to"] = ch.to
        if ch.fields is not None:
            rec["fields"] = ch.fields
        if ch.detail is not None:
            # Omit internal-only detail keys that are pipeline artefacts and
            # not part of the published contract.
            clean = {
                k: v for k, v in ch.detail.items()
                if k not in ("reorder_candidate", "before_order", "after_order")
            }
            if clean:
                rec["detail"] = clean
        records.append(rec)
    return records


def _capture_baseline(schema_dict: dict) -> dict:
    """Return a deep copy of *schema_dict* using the json round-trip idiom.

    This matches the existing ``clone()`` idiom and relies on the schema dict
    being JSON-serialisable, which is an invariant already established by the
    module.

    :param schema_dict: The schema dict to snapshot.
    :returns: An independent deep copy.
    """
    return json.loads(json.dumps(schema_dict))

class SchemaError(Exception):
    """Base exception for Schema-related errors."""
    pass

class ClassNotFoundError(SchemaError):
    """Raised when a class is not found in the schema."""
    pass

class PropertyNotFoundError(SchemaError):
    """Raised when a property is not found in a class."""
    pass

class PropertyExistsError(SchemaError):
    """Raised when attempting to create a property that already exists."""
    pass

class InvalidInversePropertyError(SchemaError):
    """Raised when an invalid inverse property is specified."""
    pass

class Schema:
    """In-memory representation of a DataGraphs domain model schema.

    A ``Schema`` instance tracks every change applied to it over its lifetime
    and exposes :meth:`change_report` to emit a deterministic, net-effect,
    semantically-annotated changelog relative to the state at construction.
    Tracking is always on and adds negligible overhead for typical schema sizes.

    Every public mutating method is **atomic (all-or-nothing)**: the model is
    snapshotted at the outermost call boundary and restored on any exception, so
    a method that raises leaves the schema completely unchanged — never a partial
    write. Compound mutations (e.g. :meth:`create_subclass`, or any
    ``apply_to_subclasses`` cascade) are covered as a single unit: their inner
    self-calls share the outer snapshot and never re-snapshot. Because a
    rolled-back operation records nothing, :meth:`change_report` never surfaces a
    change for an operation the caller saw raise.
    """

    ALL_CLASSES = '__all_classes__'

    def __init__(self, name: str = "", version: str = "") -> None:
        """Create a new empty schema.

        :param name: Model name. Defaults to ``'Domain Model'`` if empty.
        :param version: Schema version. Defaults to ``'1.0'`` if empty.
        :raises TypeError: If a ``dict`` is passed instead of keyword arguments.
        """
        if isinstance(name, dict):
            raise TypeError("Schema constructor expects keyword arguments, not a dict. Use Schema.create_from() to create a schema from a dict.")
        now = datetime.datetime.now(datetime.UTC).isoformat()
        self._schema = {
            "name": "",
            "createdDate": now,
            "lastModifiedDate": now,
            "classes": [],
        }
        self.update_schema_metadata(name, version)
        # Tracking state — initialised after construction-time metadata is set
        # so that update_schema_metadata() calls above are never recorded.
        self._change_log: list[dict] = []
        self._tracking_depth: int = 0
        self._baseline: dict = _capture_baseline(self._schema)

    @staticmethod
    def create_from(data: dict, version: str = "") -> Self:
        """Create a `Schema` from a dictionary.

        Automatically detects and converts legacy-format schemas.

        :param data: Schema dictionary (new or legacy format).
        :param version: Schema version override.
        :returns: A new `Schema` instance.
        :raises SchemaError: If the dict is missing required keys.
        """
        if Schema._is_legacy_format(data):
            data = SchemaTransformer.old_to_new(data)
        schema = Schema(version=version)
        schema._set_internal_schema(data, version)
        # Re-capture baseline from the post-transform, fully-constructed dict so
        # legacy conversion and construction-time metadata never appear as changes.
        schema._baseline = _capture_baseline(schema._schema)
        schema._change_log = []
        return schema

    def update_schema_metadata(self, name: str = "", version: str = "") -> None:
        """Update the schema's name, version, and last modified date.

        :param name: New name for the schema. If empty, the name is unchanged unless it was previously empty, in which case it defaults to 'Domain Model'.
        :param version: New version string. If empty, the version is unchanged unless it was previously empty, in which case it defaults to '1.0'.
        """
        with self._track() as outermost, self._atomic(outermost):
            self._version = version or '1.0'
            self._schema['lastModifiedDate'] = datetime.datetime.now(datetime.UTC).isoformat()
            if name or version or len(self._schema.get('name', '')) == 0:
                self._schema['name'] = f"{name or 'Domain Model'} v{self.version}"
            if outermost:
                self._record("update_schema_metadata", name=name, version=version)

    @staticmethod
    def _is_legacy_format(schema: dict) -> bool:
        """Detect whether a schema dict uses the legacy (old) format."""
        classes = schema.get('classes', [])
        if classes:
            first = classes[0]
            return 'objectProperties' in first or ('label' in first and 'type' not in first)
        return 'guid' in schema

    def _set_internal_schema(self, data: dict, version: str) -> None:
        self._validate_schema(data)
        self.update_schema_metadata(version=version)
        self._schema = data

    def _validate_schema(self, schema: dict) -> None:
        required_keys = {'name', 'createdDate', 'lastModifiedDate', 'classes'}
        if not all(key in schema for key in required_keys):
            missing_keys = required_keys - set(schema.keys())
            raise SchemaError(f"Invalid schema. Missing keys: {', '.join(missing_keys)}")

    @contextmanager
    def _track(self) -> Generator[bool, None, None]:
        """Re-entrancy depth guard for the op-log.

        Yields ``outermost=True`` only when the context manager is entered at
        depth 0 (i.e. this is the outermost public call in a re-entrant chain).
        The depth counter is always restored in ``finally`` so exceptions cannot
        leave the counter permanently incremented.

        This is a safe no-op when tracking state has not yet been initialised
        (e.g. during ``__init__``): in that case the generator simply yields
        ``False`` and does not touch any tracking attributes.

        Usage::

            with self._track() as outermost:
                # ... do the real work ...
                if outermost:
                    self._record("op_name", arg1=val1)
        """
        if getattr(self, '_change_log', None) is None:
            # Tracking state absent (called during construction) — inert no-op.
            yield False
            return

        outermost = self._tracking_depth == 0
        self._tracking_depth += 1
        try:
            yield outermost
        finally:
            self._tracking_depth -= 1

    @contextmanager
    def _atomic(self, outermost: bool) -> Generator[None, None, None]:
        """All-or-nothing guard for a multi-step mutation.

        At the OUTERMOST public-call boundary (``outermost=True``) this snapshots
        the mutable model (``self._schema["classes"]`` — the only state any
        public mutating method touches) and, on ANY exception from the body,
        restores it *before* re-raising, so no mid-apply raise can leave a partial
        write.  This guard wraps EVERY public mutating method (not just the
        property create/update paths), making the whole mutating surface
        all-or-nothing.  Combined with the success-only ``_record`` (ADR 0002), a
        rolled-back op records nothing, so ``change_report`` never surfaces a
        change for an operation the caller saw raise.

        The snapshot is taken only at the outermost boundary (reusing ``_track``'s
        re-entrancy depth guard), so nested/cascade internals never each snapshot.
        The cost is a single deep copy at mutation time, independent of cascade
        breadth — the O(descendants) cascade asymptotics are unchanged.

        When ``outermost`` is ``False`` this is an inert pass-through: the
        outermost frame already owns the snapshot for the whole re-entrant chain.
        """
        if not outermost:
            yield
            return
        snapshot = copy.deepcopy(self._schema["classes"])
        try:
            yield
        except BaseException:
            # Restore IN PLACE (slice-assign), not by rebinding to a new list,
            # so any externally-held reference to the classes list (e.g. via the
            # public ``classes`` view or a prior ``to_dict()``) stays consistent
            # with the rolled-back model after a raised mutation.
            self._schema["classes"][:] = snapshot
            raise

    def _record(self, op: str, **args) -> None:
        """Append a single op-log entry to ``_change_log``.

        This is a safe no-op when ``_change_log`` has not yet been initialised,
        so calls that happen to reach here before tracking state is set up
        (e.g. ``update_schema_metadata`` during construction) do not raise
        ``AttributeError`` and do not log anything.

        :param op: The operation name (e.g. ``"create_class"``).
        :param args: Keyword arguments carrying the intent-bearing parameters
            for the operation (names, flags — not field values).
        """
        if getattr(self, '_change_log', None) is None:
            return
        self._change_log.append({"op": op, "args": args})

    @property
    def classes(self) -> list[dict]:
        """The list of class definitions in the schema."""
        return self._schema["classes"]

    @property
    def version(self) -> str:
        """The schema version string."""
        return self._version
    
    def _make_description(self, text: str) -> dict:
        """Create a description dict in the new format."""
        return {"en": text, "@none": text}

    def _get_description_text(self, desc: Union[str, dict]) -> str:
        """Extract plain text from a description (handles both str and dict)."""
        if isinstance(desc, dict):
            return desc.get('@none', desc.get('en', ''))
        return desc or ''

    def create_class(
        self,
        class_name: str,
        description: str = "",
        parent_class_name: str = "",
        label_prop_name: str = "label",
        is_label_prop_lang_string: bool = True,
    ) -> None:
        """Create a new class in the schema.

        :param class_name: Name of the new class.
        :param description: Human-readable description.
        :param parent_class_name: Name of the parent class (for inheritance).
        :param label_prop_name: Name of the label property created by default.
        :param is_label_prop_lang_string: Whether the label property supports
            multiple languages.
        :raises SchemaError: If a class with the same name already exists.
        """
        with self._track() as outermost, self._atomic(outermost):
            existing_class = self.find_class(class_name)
            if existing_class is not None:
                raise SchemaError(f"The class '{class_name}' already exists in the schema")
            class_def = {
                "type": "Class",
                "name": class_name,
                "labelProperty": label_prop_name,
                "identifierProperty": "id",
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": label_prop_name,
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": is_label_prop_lang_string,
                        "isLabelSynonym": False
                    }
                ],
                "isAbstract": False,
            }
            if description:
                class_def['description'] = self._make_description(description)
            if parent_class_name:
                class_def['subClassOf'] = parent_class_name
            self._schema['classes'].append(class_def)
            if outermost:
                self._record("create_class", class_name=class_name)

    def create_subclass(self, class_name: str, description: str, parent_class_name: str) -> None:
        """Create a subclass that inherits all properties from the parent class.

        :param class_name: Name of the new subclass.
        :param description: Description for the subclass.
        :param parent_class_name: Name of the parent class to inherit from.
        :raises ClassNotFoundError: If the parent class does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(parent_class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Parent class '{parent_class_name}' not found")
            label_prop_name = class_def['labelProperty']
            label_prop_def = self.find_property(class_def['properties'], label_prop_name)
            self.create_class(class_name, description, parent_class_name, label_prop_name, label_prop_def.get('isLangString', False))
            for prop_def in class_def['properties']:
                if prop_def['name'] != label_prop_name:
                    validation_rules = prop_def.get('validationRules', [])
                    range_value = prop_def['range']
                    try:
                        datatype = DATATYPE(range_value)
                    except ValueError:
                        datatype = range_value
                    enums = validation_rules[0].get('value', []) if validation_rules else []
                    desc = self._get_description_text(prop_def.get('description'))
                    self.create_property(
                        class_name,
                        prop_def['name'],
                        datatype,
                        desc,
                        prop_def.get('isOptional', True),
                        prop_def.get('isArray', False),
                        prop_def.get('isNestedObject', False),
                        prop_def.get('isLangString', False),
                        prop_def.get('inverseOf', ''),
                        enums,
                        prop_def.get('isLabelSynonym', False),
                        prop_def.get('isFilterable', None),
                        apply_to_subclasses=False,
                    )
            if outermost:
                # Capture the names of the properties INHERITED from the parent
                # AT CREATION TIME (op-time intent, consistent with how
                # apply_to_subclasses captures its target set).  The report uses
                # this set — not the subclass's live property count — so a
                # property added to the subclass AFTER create_subclass is NOT
                # mislabelled "inherited" and instead surfaces as its own
                # ``added`` record (round-4 B2).
                inherited_properties = [
                    p["name"] for p in class_def.get("properties", []) if "name" in p
                ]
                self._record(
                    "create_subclass",
                    class_name=class_name,
                    parent_class_name=parent_class_name,
                    inherited_properties=inherited_properties,
                )

    def update_class(self, class_name: str, new_name: str = "", new_description: str = "", parent_class_name: str = "") -> None:
        """Update a class's name, description, or parent class.

        :param class_name: Current class name.
        :param new_name: New class name, or empty to leave unchanged.
        :param new_description: New description, or empty to leave unchanged.
        :param parent_class_name: New parent class. Empty string removes the parent.
        :raises ClassNotFoundError: If the class does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            if new_name:
                class_def["name"] = new_name
            if parent_class_name:
                class_def['subClassOf'] = parent_class_name
            elif 'subClassOf' in class_def:
                del class_def['subClassOf']
            if new_description:
                class_def['description'] = self._make_description(new_description)
            if outermost:
                self._record("update_class", class_name=class_name, new_name=new_name)

    def delete_class(self, class_name: str, include_linked_properties: bool = False, cascade_to_subclasses: bool = True) -> None:
        """Delete a class from the schema.

        :param class_name: Name of the class to delete.
        :param include_linked_properties: If ``True``, also removes ObjectProperties
            on other classes that reference this class.
        :param cascade_to_subclasses: If ``True``, removes ``subClassOf`` links
            from any subclasses of the deleted class.
        :raises ClassNotFoundError: If the class does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            self._schema["classes"].remove(class_def)
            if include_linked_properties:
                self._delete_linked_properties(class_name)
            if cascade_to_subclasses:
                for other_def in self._schema["classes"]:
                    if other_def.get("subClassOf") == class_name:
                        other_def.pop("subClassOf", None)
            if outermost:
                self._record("delete_class", class_name=class_name, cascade_to_subclasses=cascade_to_subclasses)

    def assign_label_property(self, class_name: str, prop_name: str, is_lang_string: bool = True) -> None:
        """Designate an existing property as the label property for a class.

        The property is also marked as required (``isOptional=False``).

        :param class_name: Class name.
        :param prop_name: Property name to use as the label.
        :param is_lang_string: Whether the label supports multiple languages.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist on the class.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            class_def["labelProperty"] = prop_name
            prop_def = self.find_property(class_def["properties"], prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
            prop_def["isOptional"] = False
            prop_def["isLangString"] = is_lang_string
            if outermost:
                self._record("assign_label_property", class_name=class_name, prop_name=prop_name)

    def assign_label_autogen(self, class_name: str, pattern: str) -> None:
        """Set an auto-generation pattern on the label property of a class.

        :param class_name: Class name.
        :param pattern: Auto-generation expression.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the label property does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            prop_name = class_def["labelProperty"]
            prop_def = self.find_property(class_def["properties"], prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Label property '{prop_name}' not found in class '{class_name}'")
            prop_def['propertyValuePattern'] = pattern
            if outermost:
                self._record("assign_label_autogen", class_name=class_name)

    def assign_baseclass(self, class_name: str, parent_class_name: str) -> None:
        """Set or change the parent (base) class for an existing class.

        :param class_name: The class to modify.
        :param parent_class_name: The new parent class name.
        :raises ClassNotFoundError: If *class_name* does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            class_def['subClassOf'] = parent_class_name
            if outermost:
                self._record("assign_baseclass", class_name=class_name, parent_class_name=parent_class_name)

    def assign_class_description(self, class_name: str, description: str) -> None:
        """Set or clear the description of a class.

        :param class_name: Class name.
        :param description: New description. Pass an empty string to remove it.
        :raises ClassNotFoundError: If the class does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            if description:
                class_def['description'] = self._make_description(description)
            else:
                class_def.pop('description', None)
            if outermost:
                self._record("assign_class_description", class_name=class_name)

    def _delete_linked_properties(self, class_name: str) -> None:
        for class_def in self._schema["classes"]:
            properties_to_remove = [
                prop_def for prop_def in class_def["properties"]
                if (prop_def.get("type") == "ObjectProperty"
                    and prop_def.get("range") == class_name)
            ]
            for prop_def in properties_to_remove:
                class_def["properties"].remove(prop_def)

    def create_property(
        self,
        class_name: str,
        prop_name: str,
        datatype: Union[DATATYPE, str],
        description: str = "",
        is_optional: bool = True,
        is_array: bool = False,
        is_nested: bool = False,
        is_lang_string: bool = True,
        inverse_of: str = "",
        enums: Optional[list] = None,
        is_synonym: bool = False,
        is_filterable: Optional[bool] = None,
        apply_to_subclasses: bool = False,
    ) -> None:
        """Create a new property on a class.

        :param class_name: Class to add the property to.
        :param prop_name: Property name.
        :param datatype: A `DATATYPE` enum value for primitive types, or a class
            name string for object (relationship) properties.
        :param description: Human-readable description.
        :param is_optional: Whether the property is optional.
        :param is_array: Whether the property holds multiple values.
        :param is_nested: Whether an object property is nested (embedded).
        :param is_lang_string: For text properties, whether to support multiple
            languages.
        :param inverse_of: Name of the inverse property on the target class
            (object properties only).
        :param enums: Allowed values for ``DATATYPE.ENUM`` properties.
        :param is_synonym: Whether this property is a label synonym.
        :param is_filterable: Whether the property is available as a facet/filter.
        :param apply_to_subclasses: If ``True``, also creates the property on all
            existing subclasses.
        :raises ClassNotFoundError: If the class (or referenced class) does not exist.
        :raises PropertyExistsError: If a property with the same name already exists.
        :raises InvalidInversePropertyError: If the inverse property specification
            is invalid.
        """
        with self._track() as outermost, self._atomic(outermost):
            if enums is None:
                enums = []
            if not (hasattr(datatype, 'value') and (datatype.value in set(i.value for i in DATATYPE))) and not isinstance(datatype, str):
                raise TypeError(f"Unspecified datatype for {class_name}.{prop_name}")

            # Build the by-name and parent->children indices ONCE (FIX round-4 B3):
            # a single O(C) pass replaces the per-level O(C) find_subclasses /
            # find_class scans that made the cascade O(C^2).  The target set is the
            # parent plus its transitive descendants in BFS order; it also serves
            # as the op-log's op-time intent set (FIX VR-B3).
            if apply_to_subclasses:
                by_name, children = self._class_indices()
                target_names = [class_name] + self._descendants(class_name, children)
            else:
                cdef = self.find_class(class_name)
                by_name = {class_name: cdef} if cdef is not None else {}
                target_names = [class_name]

            # Pre-validate existence / duplicate up front (cheap, O(targets) via the
            # O(1) index) so the common conflict cases raise before any mutation.
            target_defs: list[dict] = []
            for name in target_names:
                cdef = by_name.get(name)
                if cdef is None:
                    raise ClassNotFoundError(f"Class '{name}' not found")
                if self.find_property(cdef["properties"], prop_name) is not None:
                    raise PropertyExistsError(
                        f"The property '{prop_name}' already exists in the class: {name}"
                    )
                target_defs.append(cdef)

            # Apply to every target iteratively (no Python recursion, so a
            # 1000s-deep subClassOf chain cannot RecursionError; FIX B4).  This is
            # ALL-OR-NOTHING: pre-validation cannot cover every mid-apply raise
            # (inverse_of / object-range / enum / datatype validity is per-target
            # and resolved here), so the outermost `_atomic` guard snapshots the
            # model and rolls back on ANY exception — no raise leaves a partial
            # write, and `_record` (below, success-only) never lies about it.
            for cdef in target_defs:
                self._create_property_on_class(
                    cdef, cdef["name"], prop_name, datatype, description,
                    is_optional, is_array, is_nested, is_lang_string,
                    inverse_of, enums, is_synonym, is_filterable,
                )

            # The op-time intent set is the cascade footprint minus the parent
            # itself (the subclasses the op actually touched), in BFS order.
            applied_subclasses = target_names[1:] if (outermost and apply_to_subclasses) else []
            if outermost:
                self._record(
                    "create_property",
                    class_name=class_name,
                    prop_name=prop_name,
                    apply_to_subclasses=bool(apply_to_subclasses),
                    applied_subclasses=applied_subclasses,
                )

    def _create_property_on_class(
        self, class_def: dict, owner_class_name: str, prop_name: str,
        datatype: Union[DATATYPE, str], description: str, is_optional: bool,
        is_array: bool, is_nested: bool, is_lang_string: bool, inverse_of: str,
        enums: list, is_synonym: bool, is_filterable: Optional[bool],
    ) -> None:
        """Create one property on one already-resolved class dict.

        The single-class core shared by :meth:`create_property` and its cascade.
        The caller pre-validates existence/duplicate; this core may still raise
        mid-apply (``_assign_datatype`` on a missing object range,
        ``_assign_inverse_of`` on an invalid inverse) AFTER appending the
        half-built dict — the caller's outermost ``_atomic`` guard rolls the model
        back on any such raise, so the overall create is all-or-nothing.
        ``inverse_of`` is resolved against *owner_class_name* (each target's own
        class name, matching the prior per-subclass recursion's inverse validation
        exactly).
        """
        prop_def = {"name": prop_name}
        class_def["properties"].append(prop_def)
        self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
        self._assign_property_description(prop_def, description)
        self._assign_is_optional(prop_def, is_optional)
        self._assign_is_array(prop_def, is_array)
        self._assign_inverse_of(prop_def, owner_class_name, inverse_of, datatype)
        self._assign_enum(prop_def, datatype, enums)
        self._assign_is_synonym(prop_def, is_synonym)
        if is_filterable is not None:
            self._assign_is_filterable(prop_def, is_filterable)

    def _assign_property_description(self, prop_def: dict, description: str) -> None:
        if description:
            prop_def["description"] = self._make_description(description)
        else:
            prop_def.pop("description", None)

    def _assign_is_optional(self, prop_def: dict, is_optional: bool = False) -> None:
        prop_def["isOptional"] = is_optional

    def _assign_is_array(self, prop_def: dict, is_array: bool = False) -> None:
        prop_def["isArray"] = is_array

    def _assign_datatype(self, prop_def: dict, datatype: Union[DATATYPE, str], is_nested: bool = False, is_lang_string: bool = True) -> None:
        if datatype in DATATYPE:
            prop_def["type"] = "DatatypeProperty"
            prop_def["range"] = str(datatype)
            if datatype == DATATYPE.TEXT:
                prop_def["isLangString"] = is_lang_string
            else:
                prop_def["isLangString"] = False
            prop_def.pop("isNestedObject", None)
            prop_def.pop("inferLocation", None)
            prop_def.pop("isSymmetric", None)
        else:
            if self.find_class(datatype) is None:
                raise ClassNotFoundError(f"Class '{datatype}' not found for property datatype")
            prop_def["type"] = "ObjectProperty"
            prop_def["range"] = str(datatype)
            prop_def["isNestedObject"] = is_nested
            prop_def.setdefault("inferLocation", False)
            prop_def.setdefault("isSymmetric", False)
            prop_def.pop("isLangString", None)

    def _assign_inverse_of(self, prop_def: dict, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> None:
        if inverse_of and self._is_valid_inverse_of(class_name, inverse_of, datatype):
            prop_def["inverseOf"] = inverse_of

    def _assign_enum(self, prop_def: dict, datatype: Union[DATATYPE, str], enums: list) -> None:
        if datatype == DATATYPE.ENUM:
            prop_def["validationRules"] = [{
                "type": "enumeration",
                "value": enums,
            }]

    def _assign_is_filterable(self, prop_def: dict, is_filterable: Optional[bool] = None) -> None:
        if is_filterable is not None:
            prop_def["isFilterable"] = is_filterable

    def _assign_is_synonym(self, prop_def: dict, is_synonym: bool) -> None:
        if is_synonym is not None:
            prop_def["isLabelSynonym"] = is_synonym

    def _is_valid_inverse_of(self, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> bool:
        is_valid = False
        if datatype not in DATATYPE:
            class_def = self.find_class(datatype)
            if class_def is not None:
                prop_def = self.find_property(class_def["properties"], inverse_of)
                if prop_def is None:
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' not found in class '{datatype}'")
                elif prop_def.get("type") != "ObjectProperty":
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' has no range defined, expected '{class_name}'")
                elif prop_def.get("range") != class_name:
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' does not point back to class '{class_name}'")
                else:
                    is_valid = True
            else:
                raise InvalidInversePropertyError(f"Inverse property refers to non-existent class '{datatype}'")
        else:
            raise InvalidInversePropertyError(f"Inverse property can only be set for properties with concept datatype, not '{datatype}'")
        return is_valid

    def update_property(
        self,
        class_name: str,
        prop_name: str,
        datatype: Union[DATATYPE, str] = None,
        description: str = None,
        is_optional: bool = None,
        is_array: bool = None,
        is_nested: bool = None,
        is_lang_string: bool = None,
        inverse_of: str = "",
        enums: Optional[list] = None,
        is_synonym: bool = False,
        is_filterable: bool = None,
        apply_to_subclasses: bool = None,
    ) -> None:
        """Update an existing property on a class.

        Only parameters that are explicitly provided (non-``None``) will be
        changed.

        :param class_name: Class containing the property.
        :param prop_name: Property name to update.
        :param datatype: New data type.
        :param description: New description.
        :param is_optional: Whether the property is optional.
        :param is_array: Whether the property holds multiple values.
        :param is_nested: Whether an object property is nested.
        :param is_lang_string: Whether the property supports multiple languages.
        :param inverse_of: Name of the inverse property on the target class.
        :param enums: Allowed enumeration values.
        :param is_synonym: Whether this property is a label synonym.
        :param is_filterable: Whether the property is available as a filter.
        :param apply_to_subclasses: If ``True``, also updates the property on all
            existing subclasses.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            # Build the by-name and parent->children indices ONCE (FIX round-4 B3):
            # one O(C) pass, not a find_subclasses/find_class scan per level.
            if apply_to_subclasses:
                by_name, children = self._class_indices()
                target_names = [class_name] + self._descendants(class_name, children)
            else:
                cdef = self.find_class(class_name)
                by_name = {class_name: cdef} if cdef is not None else {}
                target_names = [class_name]

            # Pre-validate existence / presence up front (cheap, O(targets) via the
            # O(1) index) so the common not-found cases raise before any mutation.
            targets: list[tuple[dict, dict]] = []
            for name in target_names:
                cdef = by_name.get(name)
                if cdef is None:
                    raise ClassNotFoundError(f"Class '{name}' not found")
                pdef = self.find_property(cdef["properties"], prop_name)
                if pdef is None:
                    raise PropertyNotFoundError(
                        f"Property '{prop_name}' not found in class '{name}'"
                    )
                targets.append((cdef, pdef))

            # Apply iteratively (no Python recursion; FIX B4).  ALL-OR-NOTHING:
            # inverse_of / object-range / enum / datatype validity is per-target
            # and resolved here, so a mid-apply raise is rolled back by the
            # outermost `_atomic` guard — no partial write, and `_record` (below,
            # success-only) never lies about a raised op.
            for cdef, pdef in targets:
                self._update_property_on_class(
                    cdef, pdef, cdef["name"], datatype, description, is_optional,
                    is_array, is_nested, is_lang_string, inverse_of, enums,
                    is_synonym, is_filterable,
                )

            applied_subclasses = target_names[1:] if (outermost and apply_to_subclasses) else []
            if outermost:
                self._record(
                    "update_property",
                    class_name=class_name,
                    prop_name=prop_name,
                    apply_to_subclasses=bool(apply_to_subclasses),
                    applied_subclasses=applied_subclasses,
                )

    def _update_property_on_class(
        self, class_def: dict, prop_def: dict, owner_class_name: str,
        datatype: Union[DATATYPE, str], description, is_optional, is_array,
        is_nested, is_lang_string, inverse_of, enums, is_synonym, is_filterable,
    ) -> None:
        """Update one already-resolved property on one already-resolved class.

        The single-class core shared by :meth:`update_property` and its cascade.
        Only explicitly-provided (non-``None``) fields are changed, exactly as
        the public method.  ``inverse_of`` is resolved against *owner_class_name*
        (each target's own class name, matching the prior per-subclass recursion).
        """
        if description is not None:
            self._assign_property_description(prop_def, description)
        if is_optional is not None:
            self._assign_is_optional(prop_def, is_optional)
        if is_array is not None:
            self._assign_is_array(prop_def, is_array)
        if datatype is not None:
            self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
        if inverse_of is not None:
            self._assign_inverse_of(prop_def, owner_class_name, inverse_of, datatype)
        if enums is not None:
            existing_datatype = prop_def["range"]
            self._assign_enum(prop_def, existing_datatype, enums)
        if is_filterable is not None:
            self._assign_is_filterable(prop_def, is_filterable)
        if is_synonym is not None:
            self._assign_is_synonym(prop_def, is_synonym)

    def rename_property(self, class_name: str, old_prop_name: str, new_prop_name: str) -> None:
        """Rename a property.

        If the property is the class's label property, the label property
        reference is updated automatically.

        :param class_name: Class containing the property.
        :param old_prop_name: Current property name.
        :param new_prop_name: New property name.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If *old_prop_name* does not exist.
        :raises PropertyExistsError: If *new_prop_name* is already in use.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            prop_def = self.find_property(class_def["properties"], old_prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Property '{old_prop_name}' not found in class '{class_name}'")
            conflict_prop_def = self.find_property(class_def["properties"], new_prop_name)
            if conflict_prop_def is not None:
                raise PropertyExistsError(f"The new property name '{new_prop_name}' is already in use")
            prop_def["name"] = new_prop_name
            if class_def["labelProperty"] == old_prop_name:
                class_def["labelProperty"] = new_prop_name
            if outermost:
                self._record("rename_property", class_name=class_name, old_prop_name=old_prop_name, new_prop_name=new_prop_name)

    def delete_property(self, class_name: str, prop_name: str) -> None:
        """Remove a property from a class.

        :param class_name: Class containing the property.
        :param prop_name: Property name to delete.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist.
        """
        with self._track() as outermost, self._atomic(outermost):
            class_def = self.find_class(class_name)
            if class_def is None:
                raise ClassNotFoundError(f"Class '{class_name}' not found")
            prop_def = self.find_property(class_def["properties"], prop_name)
            if prop_def is None:
                raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
            class_def["properties"].remove(prop_def)
            if outermost:
                self._record("delete_property", class_name=class_name, prop_name=prop_name)

    def find_class(self, name: str) -> Optional[dict]:
        """Find a class definition by name.

        :param name: The class name to look up.
        :returns: The class dict, or ``None`` if not found.
        """
        return next((x for x in self._schema["classes"] if x['name'] == name), None)

    def find_subclasses(self, baseclass: str) -> list[dict]:
        """Find all direct subclasses of a given class.

        :param baseclass: The parent class name.
        :returns: A list of class dicts whose ``subClassOf`` matches *baseclass*.
        """
        return [x for x in self._schema["classes"] if x.get('subClassOf') == baseclass]

    def _children_index(self) -> dict[str, list[str]]:
        """Build the parent-name -> direct-children-names index in ONE O(C) pass.

        Built once per outermost cascade so the iterative descendant walk is
        O(descendants) rather than an O(C) ``find_subclasses`` scan per level
        (FIX round-4 B3 — the relocated op-time quadratic).
        """
        return self._class_indices()[1]

    def _class_indices(self) -> tuple[dict[str, dict], dict[str, list[str]]]:
        """Build the (name -> class_def) and (parent -> children) indices in ONE pass.

        Both indices back the cascade in O(descendants): the name index makes the
        atomic pre-validation O(targets) (an O(1) lookup per target rather than an
        O(C) ``find_class`` scan), and the children index drives the iterative
        descendant walk — together eliminating the O(C^2) cascade (FIX round-4 B3).
        """
        by_name: dict[str, dict] = {}
        children: dict[str, list[str]] = {}
        for cls in self._schema["classes"]:
            name = cls.get("name")
            if name is None:
                continue
            by_name[name] = cls
            parent = cls.get("subClassOf")
            if parent is not None:
                children.setdefault(parent, []).append(name)
        return by_name, children

    @staticmethod
    def _descendants(baseclass: str, children: dict[str, list[str]]) -> list[str]:
        """Transitive descendants of *baseclass* in BFS order, off a prebuilt index.

        ITERATIVE (explicit queue), so a ``subClassOf`` chain thousands of levels
        deep cannot exceed Python's recursion limit (FIX round-4 B4).  Each class
        is visited at most once (cycle-safe).
        """
        result: list[str] = []
        seen: set[str] = {baseclass}
        queue: deque[str] = deque(children.get(baseclass, []))
        while queue:
            name = queue.popleft()
            if name in seen:
                continue
            seen.add(name)
            result.append(name)
            queue.extend(children.get(name, []))
        return result

    def _transitive_subclass_names(self, baseclass: str) -> list[str]:
        """Names of every transitive subclass of *baseclass*, in BFS order.

        Mirrors the cascade footprint of ``apply_to_subclasses=True`` (direct
        children, their children, and so on).  A class is visited at most once
        (cycle-safe).  Builds the children index once and walks it iteratively.
        """
        return self._descendants(baseclass, self._children_index())

    def find_property(self, props: list, name: str) -> Optional[dict]:
        """Find a property by name within a list of property dicts.

        :param props: List of property dicts to search.
        :param name: The property name to look up.
        :returns: The property dict, or ``None`` if not found.
        """
        return next((x for x in props if x['name'] == name), None)

    def assign_property_orders(self, property_orders: dict) -> None:
        """Reorder properties within classes.

        Properties not listed in the order are appended at the end.

        :param property_orders: A dict mapping class names to ordered lists of
            property names.
        """
        with self._track() as outermost, self._atomic(outermost):
            for class_def in self._schema['classes']:
                if class_def['name'] in property_orders:
                    ordered_names = property_orders[class_def['name']]
                    props_by_name = {p['name']: p for p in class_def['properties']}
                    ordered = [props_by_name[n] for n in ordered_names if n in props_by_name]
                    remaining = [p for p in class_def['properties'] if p['name'] not in set(ordered_names)]
                    class_def['properties'] = ordered + remaining
            if outermost:
                self._record("assign_property_orders", property_orders={k: list(v) for k, v in property_orders.items()})

    def clone(self) -> Self:
        """Create a deep copy of the schema.

        :returns: A new independent `Schema` instance.
        """
        return Schema.create_from(json.loads(json.dumps(self._schema)))

    def change_report(self, fmt: str = "text") -> "str | list[dict]":
        """Return a net-effect changelog of all changes since construction.

        Computes the structural delta between the baseline (state at
        construction) and the current schema, then annotates it with semantic
        intent from the op-log (renames, reorders, compound ops, label-property
        assignments).  The result is deterministic: identical mutation sequences
        always yield byte-identical text and equal records regardless of dict
        insertion order.

        This method is **strictly read-only**: it never mutates ``_schema``,
        ``_baseline``, or ``_change_log``.

        **Supported surface / guarantees.**  ``fmt="records"`` is the
        fully-supported, guaranteed output: deterministic and complete — every
        structural change since the baseline is present, with its full
        ``from``/``to``/``fields``/``detail`` payload, for programmatic
        consumption.  ``fmt="text"`` is a **best-effort human-readable** rendering
        of the same change set; it is NOT guaranteed to round-trip user-supplied
        field content (e.g. a ``description`` containing newlines may produce
        additional or ambiguous lines in the text changelog) — a documented known
        limitation.  Cross-subclass annotation of ``apply_to_subclasses`` cascade
        ops in the report is likewise **best-effort**.  Prefer ``fmt="records"``
        whenever the output is parsed or relied upon.

        **Cost.**  For cascade-heavy edit histories the report is approximately
        ``O(L*C)`` (``L`` cascade ops over a parent of ``C`` subclasses): a cascade
        op genuinely fans out to one record per annotated subclass, so the report
        size — and therefore its cost — is inherent to annotating ``C`` subclasses.

        :param fmt: Output format.

            * ``"text"`` *(default)*: returns a deterministic plain-text
              changelog ``str`` with a header count line and per-class grouping.
              Best-effort human rendering — see *Supported surface* above.
            * ``"records"``: returns a ``list[dict]`` of structured change
              records for programmatic consumption — the supported, guaranteed
              output.  See *Record shape* below.

        :returns: A ``str`` when ``fmt="text"``; a ``list[dict]`` when
            ``fmt="records"``.  Returns ``""`` (text) or ``[]`` (records) when
            nothing has changed since construction.
        :raises ValueError: If *fmt* is not ``"text"`` or ``"records"``.

        .. note::

            **Untracked edits via** :meth:`to_dict` **— graceful degradation.**
            :meth:`to_dict` returns the live internal dict; mutations applied
            directly to that dict bypass the op-log entirely.  Those changes
            are still captured by the structural diff and appear in
            ``change_report`` output, but *without* semantic intent labels:
            a property rename done through the dict appears as a remove + add
            rather than a single ``renamed`` entry, an unlogged reorder does
            not become a ``reordered`` entry, and so on.  Use the public
            mutating methods to preserve full semantic annotation.

        **Record shape** (``fmt="records"``)

        Each dict always carries:

        * ``"target"`` (``str``) — dotted path of the changed entity, using
          the current name: ``"ClassName"`` for class/metadata changes or
          ``"ClassName.propName"`` for property changes.
        * ``"kind"`` (``str``) — ``"class"``, ``"property"``, or
          ``"metadata"``.
        * ``"op"`` (``str``) — one of ``"added"``, ``"removed"``,
          ``"modified"``, ``"renamed"``, ``"reordered"``,
          ``"subclass_created"``.

        The following keys are **omitted** (not ``None``) when they do not
        apply to the entry:

        * ``"from"`` (``str``) — previous name; present only when
          ``op="renamed"``.
        * ``"to"`` (``str``) — new name; present only when ``op="renamed"``.
        * ``"fields"`` (``list[dict]``) — field-level before/after list, each
          entry ``{"field": str, "before": Any, "after": Any}``; present on
          ``op="modified"`` and on ``op="renamed"`` when field-level changes
          accompany the rename.
        * ``"detail"`` (``dict``) — supplementary annotation dict; present for
          compound or annotated entries:

          - ``op="subclass_created"``: ``{"parent": str, "inherited": int}``
          - ``op="reordered"``: ``{"order": list[str]}``
          - ``op="added"`` / ``op="modified"`` with ``apply_to_subclasses``:
            ``{"applied_to_subclasses": list[str]}``
          - ``op="modified"`` (label-property assignment):
            ``{"label_property": str}``
        """
        if fmt not in ("text", "records"):
            raise ValueError(
                f"change_report() fmt must be 'text' or 'records', got {fmt!r}"
            )

        # Build the annotated change list — all on local variables; nothing
        # written back to any instance attribute.
        rename_map = _replay_identities(self._baseline, self._change_log)
        raw_changes = _diff(self._baseline, self._schema, rename_map)
        annotated = _annotate(raw_changes, self._change_log, self._baseline, self._schema, rename_map)
        # Drop any unconsumed internal "__order__" reorder sentinel: it is a
        # pipeline artefact that _annotate consumes only when a tracked
        # assign_property_orders op exists.  An UNTRACKED reorder (to_dict edit)
        # has no op-log entry, so the sentinel survives — and must never leak
        # into either output (documented degradation: untracked reorders are not
        # reported as reorders, ADR 0001).  Single chokepoint for both formats.
        annotated = [
            ch for ch in annotated
            if not (ch.detail is not None and ch.detail.get("reorder_candidate"))
        ]
        ordered = _sort_changes(annotated)

        if fmt == "text":
            return _render_text(ordered)
        return _render_records(ordered)

    def to_dict(self) -> dict:
        """Convert the schema to a plain dictionary.

        :returns: The schema as a dict.
        """
        return self._schema

    def to_json(self) -> str:
        """Serialise the schema to a JSON string.

        :returns: A JSON-formatted string.
        """
        return json.dumps(self._schema, ensure_ascii=False, indent=2)

