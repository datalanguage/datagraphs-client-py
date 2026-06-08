---
id: sol-005-regression-tests-must-assert-all-output-formats
title: "Bug-first regression tests must assert on EVERY output format, not just one"
tags: [testing, bug-first, regression, output-format, serialization, renderer, multi-format, dual-representation]
confidence: 9.5
autofix_class: approval_needed
created_utc: "2026-06-02T16:05:00Z"
source_review: "schema-change-tracking-uncommitted-2"
source_issue: "consequences-B1"
---

## Problem

When a feature exposes the *same underlying data* through more than one rendering
or serialisation format (e.g. a human-facing `text` report AND a structured
`records`/JSON list, or HTML + plaintext, or CSV + protobuf), a bug-first
regression test that asserts against **only one** format leaves the other format
unguarded. A fix can be genuinely correct in the asserted format and silently
wrong in the unasserted one — and the test suite stays green while the default,
most-visible output lies.

Concretely (this review): the `V-B2` fix correctly emitted a destructive
`removed` Change, and the bug-first tests asserted it against `fmt="records"`.
But the *default* `fmt="text"` renderer silently dropped that same `removed`
because its block-emit loop guarded on "first encounter of this class"
(`if cls_name not in emitted_class_blocks`) and treated any second class-level
Change as a duplicate to skip. The destructive recycle therefore rendered as a
benign rename in the default output — the exact original failure mode — while the
records-only test stayed green. Two independent reviewers (consequences and
assumptions) found this same class of divergence (a dropped `removed` and a
dropped `reordered`), confirming it is a *structural* test gap, not a one-off.

The deeper trap: the fix introduced a NEW invariant in the shared data
(`_diff`/`_annotate` can now emit two class-level Changes for one class), and
only one of the two renderers was updated to honour it. The format whose test
existed got fixed; the format whose test did not exist regressed.

## Solution

- For any feature with N output formats over one data model, every bug-first test
  for a data-level fix MUST assert the corrected behaviour in **all N formats**,
  or explicitly justify (in the test) why a format is exempt.
- Prefer a parametrised test that runs the same scenario through each format
  (`@pytest.mark.parametrize("fmt", ["text", "records"])`) and asserts the
  semantic signal is present in each.
- Treat the formats as a *contract pair*: add a property-style test asserting
  that the two formats agree on the *set of semantic events* they report (e.g.
  every `op` present in records is observable in text and vice versa), so a
  future divergence fails loudly regardless of which renderer drifts.
- When a fix changes an invariant in the shared/intermediate representation
  (here: "a class may now carry more than one class-level Change"), audit every
  consumer of that representation — not just the one the failing test exercised.

## When This Applies

- Any system that renders/serialises one internal model into multiple formats:
  text + JSON, CLI + API, HTML + plaintext email, CSV + structured export,
  changelogs, diff reports, audit logs.
- Especially when one format is the *default* / human-facing one and the test
  was written against the *structured* one (or vice versa) because it is easier
  to assert on.
- Indicator: a bug-first test passes, the structured output is correct, but a
  manual look at the default rendered output still shows the old wrong behaviour.
- Does NOT apply when there is genuinely a single output representation, or when
  a format is a pure lossless re-encoding verified by a round-trip property test.
