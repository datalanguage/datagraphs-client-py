---
id: sol-006-report-time-state-vs-intent-time-state
title: "Deriving an audit annotation from report-time state instead of intent-time state over- or under-claims"
tags: [audit-log, changelog, intent-vs-effect, temporal-coupling, snapshot, migration, over-claim, state-at-time, append-only-log]
confidence: 8.0
autofix_class: approval_needed
created_utc: "2026-06-02T16:05:00Z"
source_review: "schema-change-tracking-uncommitted-2"
source_issue: "intent-B2"
---

## Problem

When code records *what a user/caller intended at the time of an action* but then
reconstructs the details of that action by reading the **current (report-time)
state** of the world, the report describes a different world than the one the
action operated on. The result is a false audit claim: the report attributes to
the action things that were true *later*, not things the action actually did.

Two faces of the same bug, both seen in this review on the
`apply_to_subclasses` annotation:

- **Over-claim**: the op-log recorded "apply this update to subclasses", but the
  affected-subclass list was computed as `_subclass_current_names(parent)` — the
  subclass set *at report time*. A subclass created AFTER the call was never
  touched by the operation, yet it appeared in the operation's
  `applied_to_subclasses` list. The changelog claimed a change that demonstrably
  never happened (the new subclass still held the old value). On a migration
  changelog for a destructive-PUT platform, claiming a change that did not occur
  is as dangerous as hiding one that did.
- **Under-claim / mis-resolution**: the same report-time lookup, when a name was
  renamed-then-recycled between the call and the report, resolved to the *wrong*
  entity, found no subclasses, and silently dropped the annotation entirely.

This is a *temporal coupling* defect: an annotation about a point-in-time event
is being derived from a snapshot taken at a different point in time. It is
especially seductive because it usually fixes a narrower bug (the original
`apply_to_subclasses` annotation was missing in the common case), so the
fix-author reads current state to "find the subclasses" and the bug-first test —
which never adds a subclass after the call — passes.

## Solution

- The authoritative source for "what an action did" is the action's own recorded
  intent *plus the state as of that action*, never the final state.
- Capture the relevant set at intent time: when recording the op, snapshot the
  list it will affect (e.g. record the visited-subclass set at call time into the
  op-log entry), or record enough to reconstruct it deterministically from the
  baseline + the ordered op-log up to that entry.
- If you must read live state, intersect it with "entities that existed at or
  before the call" so post-action additions cannot leak into a past action's
  report.
- Distinguish two questions explicitly and answer each from the right source:
  "what did this action do?" (intent-time) vs. "what is true now?"
  (report-time). Mixing them in one field is the bug.
- Add a regression test whose scenario MUTATES the world after the recorded
  action (create a new child, rename-then-recycle a name) and asserts the action's
  report is unchanged by the later mutation.

## When This Applies

- Audit logs, changelogs, migration reports, activity feeds, event sourcing
  projections — anywhere an entry must describe a *past* event accurately.
- Append-only op-logs / command logs replayed against current state to enrich
  entries with detail.
- Systems where identifiers can be reused (names recycled after delete+rename),
  where membership sets grow (subclasses, group members, tags) after an action,
  or where entities are renamed between the action and the report.
- Indicator: an annotation/detail field is computed by querying live state inside
  the report/render path rather than read from the recorded event.
- Does NOT apply to fields that are *deliberately* "current value" snapshots
  (clearly labelled as such) rather than "what this action did".
