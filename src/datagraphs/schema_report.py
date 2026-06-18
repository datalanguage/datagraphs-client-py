"""Net-effect change reporting for DataGraphs schemas.

This module is the pure, read-only consumer of a
:class:`~datagraphs.schema.Schema`'s tracking state. Given the baseline
snapshot, the recorded op-log, and the current schema dict, it computes a
deterministic, semantically-annotated changelog.

It has **no dependency on the Schema class**: every function operates on plain
dicts and lists, so it can be tested and reasoned about in isolation. ``Schema``
owns *producing* the op-log (the cross-cutting tracking concern woven into its
mutating methods); this module owns *interpreting* it. The single public entry
point is :func:`build_change_report`; everything else is module-internal
pipeline detail.
"""

import json
from abc import ABC, abstractmethod
from itertools import groupby
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from datagraphs.enums import REPORT_FORMAT


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

    :ivar target: The entity affected, e.g. ``"metadata"``, ``"ClassName"``,
        or ``"ClassName.propName"``.
    :ivar kind: Broad category: ``"metadata"``, ``"class"``, or ``"property"``.
    :ivar op: The operation: ``"added"``, ``"removed"``, or ``"modified"``.
    :ivar from\\_: Before-value for scalar changes (``op="modified"`` on metadata).
    :ivar to: After-value for scalar changes.
    :ivar fields: For ``op="modified"`` class/property changes — an ordered
        list of ``{"field": str, "before": Any, "after": Any}`` dicts, one per
        changed field.  Only changed fields are included.
    :ivar detail: Optional free-form dict for supplementary annotations.
        Phase 5 uses ``{"reorder_candidate": True}`` to flag property sequences
        whose *set* is unchanged but whose *order* differs.
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

    :ivar classes: ``{baseline_class_name -> current_class_name}`` for every
        class that survived to a *different* current name (genuine rename).
        A class born after baseline (no baseline name) never appears here.
    :ivar properties: ``{(baseline_class_name, baseline_prop_name) -> current_prop_name}``
        for every property that survived to a different current name.  The key's
        class component is the owning class's *baseline* name, so a property
        rename stays correctly scoped even when its owning class was itself
        renamed.
    :ivar entry_class_resolution: Per op-log position, the final current name of
        whichever class bore that entry's call-time ``class_name`` at the
        entry's position (``None`` if the class did not survive, or the entry
        carries no class name).
    :ivar entry_prop_resolution: Per op-log position, the
        ``(final_class_name, final_prop_name)`` of the property the entry's
        call-time ``(class_name, prop_name)`` referred to at its position
        (``None`` if absent or not surviving).
    :ivar class_fate: ``{baseline_class_name -> Optional[final_current_name]}``
        for EVERY class that existed at baseline.  ``None`` means the class's
        identity was ended (deleted) during the session, so a current class of
        the same name is a *different* identity (recycle), not this one.  This
        is what lets the diff match by identity and never swallow a destructive
        delete behind a recycled name.
    :ivar prop_fate: ``{(baseline_class_name, baseline_prop_name) -> Optional[final_current_name]}``
        for every property that existed at baseline; ``None`` means its identity
        was deleted.
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
# ChangeRenderer (strategy ABC) -> render(changes) -> str | list[dict]
#     TextChangeRenderer    — plain-text changelog with header and per-class
#                             grouping; per-op line shaping via dispatch tables.
#     RecordChangeRenderer  — structured record dicts per the contract table;
#                             absent keys omitted.
#     build_change_report selects a renderer from the _RENDERERS registry by fmt.
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


def _format_field(f: dict) -> str:
    """Render one field change as ``'field: before -> after'`` (booleans lowercased).

    The shared atom behind every field-change line: class-level field lines, the
    ``[renamed]`` property tail, and the ``modified`` property suffix all compose
    this single rendering — they differ only in the surrounding prefix and join,
    not in how an individual field is shown.

    :param f: A ``{"field": str, "before": Any, "after": Any}`` change entry.
    :returns: The ``"field: before -> after"`` rendering.
    """
    return f"{f['field']}: {_bool_str(f.get('before'))} -> {_bool_str(f.get('after'))}"


class ChangeRenderer(ABC):
    """Strategy that renders an ordered list of :class:`Change` instances.

    A renderer is the presentation half of the report pipeline: the structural
    diff and semantic annotation upstream are format-agnostic, and each concrete
    renderer turns the single sorted :class:`Change` sequence into one output
    format.  :func:`build_change_report` selects the renderer for the requested
    ``fmt`` and is the only caller; renderers hold no state between calls.

    Return type intentionally varies by format (``str`` for text, ``list[dict]``
    for records) — the contract is "produce the report in my format", and each
    concrete subclass narrows the return type in its own docstring.
    """

    @abstractmethod
    def render(self, changes: list["Change"]) -> "str | list[dict]":
        """Render *changes* (already sorted) into this renderer's output format."""
        raise NotImplementedError


class RecordChangeRenderer(ChangeRenderer):
    """Render Changes as structured record dicts (the ``records`` format).

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
    keeping records compact and stable.  This is a flat one-record-per-Change
    mapping in the supplied sorted order — identical ordering to the text format.
    """

    #: Internal-only ``detail`` keys that are pipeline artefacts, not part of the
    #: published records contract, and so are stripped from emitted records.
    _INTERNAL_DETAIL_KEYS: frozenset[str] = frozenset(
        {"reorder_candidate", "before_order", "after_order"}
    )

    def render(self, changes: list["Change"]) -> list[dict]:
        """Render *changes* as a deterministic list of record dicts.

        :param changes: Ordered :class:`Change` instances (already sorted).
        :returns: Deterministic list of record dicts.
        """
        return [self._to_record(ch) for ch in changes]

    def _to_record(self, ch: "Change") -> dict:
        """Convert a single Change to its record dict, omitting inapplicable keys."""
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
            clean = {
                k: v for k, v in ch.detail.items()
                if k not in self._INTERNAL_DETAIL_KEYS
            }
            if clean:
                rec["detail"] = clean
        return rec


class TextChangeRenderer(ChangeRenderer):
    """Render Changes as a human-readable plain-text changelog (the ``text`` format).

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

    Unlike the flat records format, text output is **hierarchical**: property
    changes nest under their owning class's block.  :meth:`render` owns that
    grouping/emission orchestration; per-op line shaping is delegated through
    the :attr:`_CLASS_HEADERS` / :attr:`_PROP_LINES` dispatch tables so that
    adding a new op is one table entry rather than another conditional branch.

    Every method is side-effect-free — it *returns* its lines rather than
    mutating shared state — so each renders in isolation and composes cleanly.
    """

    def __init__(self) -> None:
        # Per-op dispatch tables (built once per instance from bound methods /
        # closures).  Keyed on ``Change.op``; the ``modified`` builder is the
        # fallback for any op not explicitly listed, preserving the old if/elif
        # ladder's terminal ``else`` branch.
        self._CLASS_HEADERS: dict[str, Callable[["Change"], str]] = {
            "added": lambda ch: f"+ {ch.target} [new class]",
            "subclass_created": self._subclass_created_header,
            "removed": lambda ch: f"- {ch.target} [removed]",
            "reordered": lambda ch: f"~ {ch.target} [reordered]",
            "renamed": lambda ch: f"~ {ch.target} [renamed from {ch.from_ or '?'}]",
            "modified": lambda ch: f"~ {ch.target} [modified]",
        }
        self._PROP_LINES: dict[str, Callable[["Change", str], str]] = {
            "added": lambda pch, name: f"  + {name} [added]",
            "removed": lambda pch, name: f"  - {name} [removed]",
            "renamed": self._renamed_prop_line,
            "modified": self._modified_prop_line,
        }

    def render(self, changes: list["Change"]) -> str:
        """Render *changes* as a deterministic plain-text changelog string.

        :param changes: Ordered :class:`Change` instances (already sorted).
        :returns: Deterministic plain-text changelog string.
        """
        # Bucket changes by owning class ONCE so each class's full block (its
        # class-level Changes plus its nested property lines) renders inline on
        # first encounter, in the supplied sorted order.
        class_changes: dict[str, list["Change"]] = {}
        prop_changes: dict[str, list["Change"]] = {}
        for ch in changes:
            if ch.kind == "class":
                class_changes.setdefault(ch.target, []).append(ch)
            elif ch.kind == "property":
                prop_changes.setdefault(self._owning_class(ch), []).append(ch)

        lines: list[str] = []
        header = self._summary_header(changes)
        if header is not None:
            lines.append(header)

        emitted: set[str] = set()
        for ch in changes:
            if ch.kind == "metadata":
                lines.append(self._metadata_line(ch))
                continue
            cls_name = ch.target if ch.kind == "class" else self._owning_class(ch)
            if cls_name not in emitted:
                emitted.add(cls_name)
                lines.extend(self._class_block(
                    cls_name,
                    class_changes.get(cls_name, []),
                    prop_changes.get(cls_name, []),
                ))

        return "\n".join(lines)

    @staticmethod
    def _owning_class(ch: "Change") -> str:
        """The class name owning a property Change (``"Class"`` from ``"Class.prop"``)."""
        return ch.target.split(".")[0]

    @staticmethod
    def _summary_header(changes: list["Change"]) -> Optional[str]:
        """The ``Schema changes (N):`` header, or ``None`` when there is nothing.

        *N* counts one entry per metadata change plus one per distinct owning
        class that has any change (property-only changed classes count once);
        individual property lines are not counted.
        """
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
        return f"Schema changes ({top_level_n}):" if top_level_n > 0 else None

    @staticmethod
    def _metadata_line(ch: "Change") -> str:
        """The single ``~ target: before -> after`` line for a metadata Change."""
        return f"~ {ch.target}: {_bool_str(ch.from_)} -> {_bool_str(ch.to)}"

    def _class_block(
        self,
        cls_name: str,
        class_chs: list["Change"],
        prop_chs: list["Change"],
    ) -> list[str]:
        """Render ALL class-level Changes for *cls_name*, then its property lines.

        FIX VR-B1: a class may legitimately carry MORE THAN ONE class-level
        Change (a recycled name yields renamed + removed; a concurrent reorder
        yields modified/renamed + reordered).  Every such Change must render —
        the renderer must not silently drop a destructive ``removed`` or a
        ``reordered`` after the first block, or the text output would lie while
        records stay correct.  A class with only property-level changes gets a
        synthesised ``~`` header.
        """
        lines: list[str] = []
        if class_chs:
            for class_ch in class_chs:
                lines.extend(self._one_class_change(class_ch))
        else:
            lines.append(f"~ {cls_name} [modified]")
        for pch in prop_chs:
            lines.extend(self._prop_change(pch))
        return lines

    def _one_class_change(self, ch: "Change") -> list[str]:
        """Header line + field/reorder/detail continuation lines for one class Change."""
        lines = [self._class_header(ch)]
        if ch.op in ("modified", "renamed"):
            lines.extend(f"  {_format_field(f)}" for f in (ch.fields or []))
        elif ch.op == "reordered":
            order = (ch.detail or {}).get("order", [])
            lines.append(f"  properties reordered: {order}")
        lines.extend(self._detail_lines(ch, indent="  "))
        return lines

    def _class_header(self, ch: "Change") -> str:
        """Build the single class-level header line, dispatched on ``ch.op``."""
        builder = self._CLASS_HEADERS.get(ch.op, self._CLASS_HEADERS["modified"])
        return builder(ch)

    @staticmethod
    def _subclass_created_header(ch: "Change") -> str:
        detail = ch.detail or {}
        parent = detail.get("parent", "?")
        inherited = detail.get("inherited", 0)
        return f"+ {ch.target} [new subclass of {parent}] (+{inherited} inherited)"

    def _prop_change(self, pch: "Change") -> list[str]:
        """Property change line plus any annotation continuation lines.

        A ``modified`` property carrying neither field changes nor renderable
        detail is content-free noise (it would print ``~ prop`` asserting a
        change while showing none) and is skipped (B2).  The corresponding
        ``records`` entry is likewise contentless, so both formats agree.
        """
        detail_lines = self._detail_lines(pch, indent="    ")
        if pch.op == "modified" and not pch.fields and not detail_lines:
            return []
        return [self._prop_line(pch), *detail_lines]

    def _prop_line(self, pch: "Change") -> str:
        """Single indented property-change line, dispatched on ``pch.op``."""
        prop_name = pch.target.split(".", 1)[1] if "." in pch.target else pch.target
        builder = self._PROP_LINES.get(pch.op, self._PROP_LINES["modified"])
        return builder(pch, prop_name)

    @staticmethod
    def _renamed_prop_line(pch: "Change", prop_name: str) -> str:
        parts = [f"  ~ {prop_name}: {pch.from_} -> {pch.to} [renamed]"]
        parts.extend(f"; {_format_field(f)}" for f in (pch.fields or []))
        return "".join(parts)

    @staticmethod
    def _modified_prop_line(pch: "Change", prop_name: str) -> str:
        field_parts = [_format_field(f) for f in (pch.fields or [])]
        if field_parts:
            return f"  ~ {prop_name}: " + "; ".join(field_parts)
        return f"  ~ {prop_name}"

    @staticmethod
    def _detail_lines(ch: "Change", indent: str) -> list[str]:
        """Indented continuation lines for an entry's semantic annotations.

        The default (``text``) output MUST surface **every** detail dimension
        the ``records`` format carries, or the two renderings describe different
        logical changes and the cross-format invariant policing them is blind to
        the gap.  The five published detail dimensions
        (``applied_to_subclasses``, ``label_property``, ``order``, ``parent``,
        ``inherited``) each render on their own continuation line.

        List/string values are JSON-encoded (not naively joined on a comma)
        so a value containing the delimiter (e.g. a class name with a comma)
        round-trips unambiguously and cannot fool the invariant's text-side
        parser.  The ``order``/``parent``/``inherited`` lines are continuation
        detail emitted *in addition to* the human-readable header summary (which
        keeps the familiar ``[new subclass of P] (+N inherited)`` /
        ``[reordered]`` shape), so the text contract gains content without
        losing its existing form.

        :param ch: The change whose detail to render.
        :param indent: Leading whitespace (two spaces at class level, four at
            property level) so the line nests under its owning block.
        :returns: Zero or more annotation lines.
        """
        detail = ch.detail or {}
        out: list[str] = []
        applied = detail.get("applied_to_subclasses")
        if applied:
            out.append(f"{indent}applied to subclasses: {json.dumps(applied)}")
        if detail.get("label_property") is not None:
            out.append(
                f"{indent}designated label property: "
                f"{json.dumps(detail['label_property'])}"
            )
        if detail.get("order") is not None:
            out.append(f"{indent}reorder sequence: {json.dumps(detail['order'])}")
        if detail.get("parent") is not None:
            out.append(f"{indent}subclass of: {json.dumps(detail['parent'])}")
        if detail.get("inherited") is not None:
            out.append(f"{indent}inherited count: {json.dumps(detail['inherited'])}")
        return out


#: Output-format registry: maps a :class:`REPORT_FORMAT` to its renderer
#: strategy.  Adding a format is one entry here plus its :class:`ChangeRenderer`
#: subclass — no branching in :func:`build_change_report`, which coerces the
#: requested format to the enum and selects the strategy by lookup.
_RENDERERS: dict[REPORT_FORMAT, type[ChangeRenderer]] = {
    REPORT_FORMAT.TEXT: TextChangeRenderer,
    REPORT_FORMAT.RECORDS: RecordChangeRenderer,
}


def build_change_report(
    baseline: dict,
    change_log: list[dict],
    current: dict,
    fmt: REPORT_FORMAT = REPORT_FORMAT.TEXT,
) -> "str | list[dict]":
    """Compute a net-effect changelog from a schema's tracking state.

    This is the single public entry point of the report module: it runs the
    full pipeline (identity replay -> structural diff -> semantic annotation ->
    sentinel pruning -> ordering -> rendering) and is invoked by
    :meth:`datagraphs.schema.Schema.change_report`. It is **strictly read-only**:
    it never mutates *baseline*, *change_log*, or *current*.

    :param baseline: The schema dict as captured at construction
        (``Schema._baseline``).
    :param change_log: The recorded op-log (``Schema._change_log``).
    :param current: The live schema dict (``Schema._schema``).
    :param fmt: A :class:`REPORT_FORMAT` (or an equivalent string, since the enum
        is a :class:`~enum.StrEnum`): :attr:`REPORT_FORMAT.TEXT` for the
        best-effort human-readable changelog, or :attr:`REPORT_FORMAT.RECORDS` for
        the guaranteed structured ``list[dict]``.
    :returns: A ``str`` for :attr:`REPORT_FORMAT.TEXT`; a ``list[dict]`` for
        :attr:`REPORT_FORMAT.RECORDS`. Empty (``""`` / ``[]``) when nothing changed.
    :raises ValueError: If *fmt* is not a member (or value) of :class:`REPORT_FORMAT`.
    """
    # Coerce to the enum: this validates AND normalises in one step — a member
    # passes through, a valid string ("text"/"records") maps to its member, and
    # anything else raises ValueError.  The registry lookup below is then a clean,
    # always-present enum-keyed dispatch.
    try:
        fmt = REPORT_FORMAT(fmt)
    except ValueError:
        raise ValueError(
            f"change_report() fmt must be one of "
            f"{[f.value for f in REPORT_FORMAT]}, got {fmt!r}"
        ) from None
    renderer = _RENDERERS[fmt]()
    rename_map = _replay_identities(baseline, change_log)
    raw_changes = _diff(baseline, current, rename_map)
    annotated = _annotate(raw_changes, change_log, baseline, current, rename_map)
    # Drop any unconsumed internal "__order__" reorder sentinel: it is a pipeline
    # artefact that _annotate consumes only when a tracked assign_property_orders
    # op exists. An UNTRACKED reorder (to_dict edit) has no op-log entry, so the
    # sentinel survives — and must never leak into either output (documented
    # degradation: untracked reorders are not reported as reorders, ADR 0001).
    annotated = [
        ch for ch in annotated
        if not (ch.detail is not None and ch.detail.get("reorder_candidate"))
    ]
    ordered = _sort_changes(annotated)
    return renderer.render(ordered)
