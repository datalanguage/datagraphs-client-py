"""Change-tracking and atomic-transaction machinery for schema mutations.

This module owns the subsystem that sits *between* schema encoding and schema
reporting:

* it is the **producer** of the data ``datagraphs.schema_report`` consumes —
  the baseline snapshot (:attr:`ChangeTracker.baseline`) and the intent-bearing
  op-log (:attr:`ChangeTracker.change_log`); and
* it makes every public schema mutation **all-or-nothing** via a
  footprint-scoped undo journal (:meth:`ChangeTracker.atomic` plus the
  ``stage*`` helpers and the re-entrancy guard :meth:`ChangeTracker.track`).

Separating it from :class:`datagraphs.schema.Schema` keeps three concerns in
three homes: ``Schema`` *encodes* the domain model, :class:`ChangeTracker`
*tracks and transacts* the edits, and ``schema_report`` *reports* them.

:class:`ChangeTracker` is a tight collaborator of ``Schema``: rollback restores
``schema_dict["classes"]`` in place, so the tracker holds a live reference to the
schema dict it guards. ``Schema`` constructs a fresh tracker whenever it rebinds
that dict (the ``create_from`` path), keeping the reference current.
"""

import copy
import json
from contextlib import contextmanager
from typing import Generator, Optional


class ChangeTracker:
    """Owns a schema's op-log, baseline snapshot, and atomic-rollback transaction.

    A tracker is bound to a single schema dict at construction and snapshots it
    as the baseline. Thereafter the owning :class:`~datagraphs.schema.Schema`
    drives it: it wraps each public mutation in :meth:`track` + :meth:`atomic`,
    journals touched state with the ``stage*`` helpers, and calls :meth:`record`
    exactly once per successful outermost mutation. The accumulated
    :attr:`baseline` and :attr:`change_log` are then handed to
    ``schema_report.build_change_report`` to produce the changelog.
    """

    def __init__(self, schema_dict: dict) -> None:
        """Bind the tracker to *schema_dict* and capture it as the baseline.

        :param schema_dict: The live schema dict to track and guard. The tracker
            keeps this reference so :meth:`atomic` can roll back its ``classes``
            list in place.
        """
        self._schema = schema_dict
        self._change_log: list[dict] = []
        self._tracking_depth: int = 0
        # Atomic-rollback transaction state (non-None only inside an outermost
        # `atomic`): the reverse-replayed undo journal and its id() dedupe sets.
        self._undo: Optional[list[tuple]] = None
        self._staged_classes: Optional[set[int]] = None
        self._staged_props: Optional[set[int]] = None
        self._baseline: dict = self._capture_baseline(schema_dict)

    @property
    def baseline(self) -> dict:
        """The schema snapshot captured at construction — the diff's *before*."""
        return self._baseline

    @property
    def change_log(self) -> list[dict]:
        """The recorded op-log: one entry per successful outermost mutation."""
        return self._change_log

    @property
    def depth(self) -> int:
        """Current re-entrancy depth (0 when no mutation is in progress)."""
        return self._tracking_depth

    @staticmethod
    def _capture_baseline(schema_dict: dict) -> dict:
        """Return an independent deep copy of *schema_dict*.

        Uses the json round-trip idiom, which relies on the schema dict being
        JSON-serialisable — an invariant the schema class already establishes.

        :param schema_dict: The schema dict to snapshot.
        :returns: An independent deep copy.
        """
        return json.loads(json.dumps(schema_dict))

    @contextmanager
    def track(self) -> Generator[bool, None, None]:
        """Re-entrancy depth guard for the op-log.

        Yields ``outermost=True`` only when entered at depth 0 (i.e. this is the
        outermost public call in a re-entrant chain). The depth counter is always
        restored in ``finally`` so exceptions cannot leave it incremented.

        Usage::

            with tracker.track() as outermost, tracker.atomic(outermost):
                # ... do the real work ...
                if outermost:
                    tracker.record("op_name", arg1=val1)
        """
        outermost = self._tracking_depth == 0
        self._tracking_depth += 1
        try:
            yield outermost
        finally:
            self._tracking_depth -= 1

    @contextmanager
    def atomic(self, outermost: bool) -> Generator[None, None, None]:
        """All-or-nothing guard for a multi-step mutation.

        At the OUTERMOST public-call boundary (``outermost=True``) this opens a
        rollback transaction over the mutable model (``schema_dict["classes"]``
        — the only state any public mutating method touches) and, on ANY
        exception from the body, restores it *before* re-raising, so no mid-apply
        raise can leave a partial write.  This guard wraps EVERY public mutating
        method (not just the property create/update paths), making the whole
        mutating surface all-or-nothing.  Combined with the success-only
        :meth:`record` (ADR 0002), a rolled-back op records nothing, so
        ``change_report`` never surfaces a change for an operation the caller saw
        raise.

        **Footprint-scoped, undo-journalled rollback.**  Rollback state is
        captured so its cost is proportional to what the operation actually
        touches, not to the size of the whole schema:

        * a SHALLOW copy of the class list (``list(classes)``) captures
          membership, order and identity, so any append / removal / reordering of
          *classes* is undoable in O(C) cheap references — no per-class deep copy;
          and
        * per-touch CONTENT is journalled lazily at the FINEST granularity the
          mutation needs — a single appended property (:meth:`track_added_prop`,
          O(1) undo), a single in-place-modified property
          (:meth:`stage_prop`, deep copy of just that small prop dict), or a
          whole class dict's scalar/structure (:meth:`stage`, deep copy of one
          class) for class-level edits.

        Granularity matters on the cascade hot path: ``create_property`` /
        ``update_property`` with ``apply_to_subclasses=True`` touch *one property
        per class* across the whole hierarchy.  Journalling that single property
        per target is O(1), so a cascade is O(C) regardless of how many properties
        each class already carries.  The prior design deep-copied each entire
        class dict (O(properties)), making a sequence of L wide cascades on C
        classes cost O(L²·C); property-granular journalling makes it O(L·C).
        Likewise, building an N-class schema one mutation at a time is O(N), not
        the O(N²) of snapshotting the whole class list per mutation.  The
        O(descendants) cascade asymptotics are unchanged.

        On rollback the journal is replayed in REVERSE (so later edits undo before
        earlier ones), then list membership/order/identity is restored by
        slice-assignment (not rebinding), so any externally-held reference to the
        classes list (e.g. via the public ``classes`` view or a prior
        ``to_dict()``) stays consistent with the rolled-back model.  In-place
        restores (``clear`` + ``update``) preserve dict identity, so references to
        individual class/property dicts also stay valid.

        When ``outermost`` is ``False`` this is an inert pass-through: the
        outermost frame already owns the transaction for the whole re-entrant
        chain, so nested/cascade internals neither snapshot nor journal twice.
        """
        if not outermost:
            yield
            return
        classes = self._schema["classes"]
        structural = list(classes)          # O(C) references — no deep copy
        self._undo = []                     # reverse-replayed rollback journal
        self._staged_classes = set()        # id() dedupe for class-level stages
        self._staged_props = set()          # id() dedupe for prop-level stages
        try:
            yield
        except BaseException:
            # Replay the journal in reverse, then restore list structure.  Each
            # entry is a (kind, *payload) tuple; in-place restores preserve dict
            # identity so external references stay valid.
            for entry in reversed(self._undo):
                kind = entry[0]
                if kind == "added":                 # ("added", props_list, prop_def)
                    _, props_list, prop_def = entry
                    try:
                        props_list.remove(prop_def)
                    except ValueError:
                        pass                        # already gone — nothing to undo
                else:                               # ("class"|"prop", target, pristine)
                    _, target, pristine = entry
                    target.clear()
                    target.update(pristine)
            classes[:] = structural
            raise
        finally:
            self._undo = None
            self._staged_classes = None
            self._staged_props = None

    def stage(self, class_def: dict) -> None:
        """Journal a whole class dict's content for atomic rollback.

        Call this BEFORE the first class-level mutation of *class_def* (a change
        to a scalar key such as ``name`` / ``subClassOf`` / ``labelProperty`` /
        ``description``, or a reordering/removal within its ``properties`` list).
        The first call for a given class dict deep-copies it; later calls for the
        same dict are no-ops, so the cost is one deep copy per distinct class the
        operation edits at class level.

        For the property-cascade hot path prefer the finer-grained
        :meth:`track_added_prop` / :meth:`stage_prop`, which journal a single
        property rather than the whole (potentially property-heavy) class dict.

        Outside an atomic transaction (``_undo is None`` — e.g. a nested call
        whose outermost frame has already closed) this is an inert no-op.
        Newly-appended class dicts need not be staged: the shallow structural
        snapshot in :meth:`atomic` drops them on rollback regardless.
        """
        if self._undo is None:
            return
        key = id(class_def)
        if key not in self._staged_classes:
            self._staged_classes.add(key)
            self._undo.append(("class", class_def, copy.deepcopy(class_def)))

    def stage_prop(self, prop_def: dict) -> None:
        """Journal a single property dict's content for atomic rollback.

        Call this BEFORE the first in-place mutation of *prop_def* (the
        cascade-update hot path).  Deep-copies only the small property dict, not
        its owning class, so an N-target cascade costs O(N) rather than
        O(N · properties-per-class).  Deduped per property dict; inert outside an
        atomic transaction.
        """
        if self._undo is None:
            return
        key = id(prop_def)
        if key not in self._staged_props:
            self._staged_props.add(key)
            self._undo.append(("prop", prop_def, copy.deepcopy(prop_def)))

    def track_added_prop(self, props_list: list, prop_def: dict) -> None:
        """Journal a freshly-appended property so rollback can remove it (O(1)).

        Call this immediately after appending *prop_def* to *props_list* (the
        cascade-create hot path).  Recording the append rather than deep-copying
        the owning class keeps a wide create-cascade O(C); on rollback the prop is
        simply removed.  Inert outside an atomic transaction.
        """
        if self._undo is None:
            return
        self._undo.append(("added", props_list, prop_def))

    def record(self, op: str, **args) -> None:
        """Append a single op-log entry to the change log.

        :param op: The operation name (e.g. ``"create_class"``).
        :param args: Keyword arguments carrying the intent-bearing parameters
            for the operation (names, flags — not field values).
        """
        self._change_log.append({"op": op, "args": args})
