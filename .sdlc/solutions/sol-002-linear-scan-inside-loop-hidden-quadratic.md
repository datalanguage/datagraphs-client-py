---
id: sol-002-linear-scan-inside-loop-hidden-quadratic
title: "Linear find() inside an outer loop is hidden O(n*m) — index once instead"
tags: [performance, algorithmic-complexity, quadratic, index-map, big-o, hot-path]
confidence: 9.0
autofix_class: approval_needed
created_utc: "2026-06-02T14:30:00Z"
source_review: "schema-change-tracking-uncommitted"
source_issue: "B1"
---

## Problem

A helper that does a linear scan to locate an item by some key
(`for i, x in enumerate(items): if predicate(x): return i`) looks innocent in
isolation — it is O(D). But when it is invoked **inside a loop** over a second
collection (size L), and especially inside a *nested* loop (per-subclass,
per-row), total cost becomes O(L·D) or O(L·S·D). This is a genuine O(n²)
pattern even when each individual scan is "small".

The trap: the outer collection's size is often driven by something the unit
tests keep small (edit history, op-log length, request count) rather than by the
data-structure size everyone reasons about. Tests exercise L in the tens, so the
quadratic never surfaces; production hits L in the thousands and the call hangs.

```python
def _find_index(predicate):
    for i, ch in enumerate(result):   # O(D)
        if predicate(ch): return i
for entry in change_log:              # O(L)  -> O(L*D) overall
    idx = _find_index(...)            # nested per-subclass -> O(L*S*D)
```

## Solution

Build O(1) lookup index maps **once** before the loop, keyed by the fields the
predicate matches on, then patch the index as items are replaced:

```python
by_target: dict[tuple[str, str], int] = {}
for i, ch in enumerate(result):
    by_target[(ch.kind, ch.target)] = i
# inside the loop: idx = by_target.get((kind, target))   # O(1)
```

This collapses O(L·D) to O(L+D). Preserve any in-place-replacement / tombstone
semantics by updating the index when an entry is replaced or nulled.

## When This Applies

- Any language. A "find by key" linear scan called inside a loop, comprehension,
  or recursive walk over a second collection.
- Strong indicator: the outer loop iterates an append-only log, an event stream,
  a request batch, or any collection whose length grows with history/usage
  rather than with the modelled entity count.
- Especially dangerous when there is an explicit "no O(n²)" NFR and the diff
  satisfies it for the obvious data structure but violates it via the history
  dimension.
- Does NOT apply when both collections are provably bounded small by invariant.
