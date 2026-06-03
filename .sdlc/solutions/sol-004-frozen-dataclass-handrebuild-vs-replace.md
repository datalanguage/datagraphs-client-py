---
id: sol-004-frozen-dataclass-handrebuild-vs-replace
title: "Editing a frozen dataclass by hand-rebuilding all fields silently drops new fields"
tags: [python, dataclass, frozen, dataclasses-replace, immutability, drift, maintainability]
confidence: 8.0
autofix_class: approval_needed
created_utc: "2026-06-02T14:30:00Z"
source_review: "schema-change-tracking-uncommitted"
source_issue: "M3"
---

## Problem

A `@dataclass(frozen=True)` cannot be mutated in place, so code that needs to
change one field often reconstructs the whole object, copying every other field
by hand:

```python
result[i] = Change(
    target=ch.target, kind=ch.kind, op=ch.op,
    from_=ch.from_, to=ch.to, fields=ch.fields,
    detail=new_detail,   # the only field actually changing
)
```

When this hand-rebuild appears in several places (here: four sites), it is pure
boilerplate AND a latent drift bug: the day someone adds a 7th field to the
dataclass, they must remember to thread it through every rebuild site, or it is
**silently dropped** (reset to its default) on every "edited" instance. There is
no compiler error — the object is just quietly wrong.

## Solution

Use `dataclasses.replace`, which copies all fields and overrides only the ones
you name:

```python
from dataclasses import replace
result[i] = replace(ch, detail=new_detail)
```

Adding a new field to the dataclass is then automatically safe at every edit
site — `replace` carries it through without any code change.

## When This Applies

- Python, any `@dataclass(frozen=True)` (or otherwise immutable record) that is
  "edited" by constructing a fresh copy with one or two changed fields.
- Strong indicator: the same multi-field constructor call is repeated in more
  than one place, differing only in which single field is overridden.
- Does NOT apply when you genuinely want a fresh instance with most fields reset,
  or when the type is not a dataclass (no `replace` available — but a `with_x()`
  helper achieves the same intent-preserving goal).
