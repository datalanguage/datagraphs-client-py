---
id: sol-001-shallow-dict-copy-aliases-inner-lists
title: "dict(x) is a shallow copy — inner lists/dicts stay aliased to the caller"
tags: [python, copy, aliasing, immutability, op-log, audit-log, defensive-copy]
confidence: 9.0
autofix_class: safe_auto
created_utc: "2026-06-02T14:30:00Z"
source_review: "schema-change-tracking-uncommitted"
source_issue: "B1"
---

## Problem

When recording the arguments of a call into an immutable log/journal (op-log,
audit trail, event record), `dict(arg)` or `arg.copy()` is a **shallow** copy.
The top-level dict is duplicated, but the inner containers (lists, nested dicts)
remain the *same objects* the caller holds. After the call returns, if the
caller mutates an inner list (`arg["k"].append(...)`), the "recorded" entry
mutates with it — silently corrupting a record that was supposed to be an
immutable snapshot of call-time intent.

```python
self._record("assign_property_orders", property_orders=dict(property_orders))
# caller does: property_orders["Animal"].append("X")
# -> the recorded entry now also ends in "X"
```

The bug is invisible in tests that only **reassign the key**
(`orders["Animal"] = [...]`) — a shallow copy survives value reassignment — so a
test written to guard the invariant can pass while the defect is wide open. The
test must mutate the *inner* container to exercise shallow-vs-deep.

## Solution

Deep-copy the inner containers when recording:

```python
property_orders={k: list(v) for k, v in property_orders.items()}
# or, for arbitrarily nested JSON-native data:
property_orders=json.loads(json.dumps(property_orders))
# or, type-preserving:
property_orders=copy.deepcopy(property_orders)
```

And write the guard test to mutate the **inner** list, not reassign the key:

```python
orders["Animal"].append("extra")   # exercises the inner-list aliasing
assert recorded == {"Animal": ["age", "label"]}   # fails on shallow copy
```

A correct guard test FAILS against the shallow-copy code and PASSES once the
deep copy is in place.

## When This Applies

- Python, any time a mutable argument is stored into a log/journal/record that
  must be an immutable snapshot of call-time state.
- Indicators: `dict(x)`, `x.copy()`, `list(x)` used as a "defensive copy" of a
  structure that contains nested mutable containers.
- Also a test-validity smell: an independence/immutability test that only
  reassigns a top-level key instead of mutating a nested container.
- Does NOT apply when the stored structure is flat (only immutable leaves) — a
  shallow copy is sufficient there.
