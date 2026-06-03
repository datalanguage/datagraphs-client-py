---
id: sol-003-json-roundtrip-not-value-preserving-deepcopy
title: "json.loads(json.dumps(x)) is not a value-preserving deep copy"
tags: [python, json, deep-copy, serialization, tuples, dict-keys, phantom-diff]
confidence: 3.0
autofix_class: approval_needed
created_utc: "2026-06-02T14:30:00Z"
source_review: "schema-change-tracking-uncommitted"
source_issue: "B6"
---

## Problem

`json.loads(json.dumps(x))` is a common deep-copy idiom, but it is **not
value-preserving**. The round-trip silently normalises several Python types:

- **tuples become lists** (`(1, 2)` -> `[1, 2]`)
- **non-string dict keys become strings** (`{1: "a"}` -> `{"1": "a"}`)
- **`NaN`/`Infinity`** survive Python's `dumps` but are non-standard JSON

If the copy is later compared with `!=` against the original (e.g. a baseline
snapshot diffed against the live object), these type-only differences fire
**phantom "modified" diffs** with visually-identical before/after values — a bug
that is invisible by inspection and only triggers when a caller injects a tuple
or non-string-keyed dict into the structure.

(Note: `int` vs `float` is NOT at risk — JSON preserves both across a round-trip,
so `1` stays `1` and `1.0` stays `1.0`.)

## Solution

- If the copy will be compared for equality against the live representation, use
  `copy.deepcopy(x)` — it is identity/type-preserving.
- If you genuinely want wire-format normalisation (e.g. `clone()` producing a
  canonical JSON-native form for transmission), the JSON round-trip is correct —
  but document that intent explicitly and do not conflate it with a
  general-purpose deep copy.
- If you keep the JSON idiom for a baseline, document the invariant: "all values
  must be JSON-native" and ideally enforce it.

## When This Applies

- Python, any deep copy made via `json.loads(json.dumps(...))` whose result is
  later compared with the original for change detection / equality.
- Indicators: a baseline/snapshot captured via the JSON idiom, diffed against a
  live mutable object, with an invariant like "a freshly loaded object reports
  zero changes".
- Does NOT apply when the structure is guaranteed JSON-native (only str keys,
  lists, JSON scalars) AND no equality comparison against a non-round-tripped
  twin occurs.
