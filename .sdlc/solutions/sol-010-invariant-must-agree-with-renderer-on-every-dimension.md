---
id: sol-010-invariant-must-agree-with-renderer-on-every-dimension
title: "A cross-format invariant and the renderers it checks must agree on EVERY field, or a symmetric omission makes the guard vacuous"
tags: [testing, invariants, cross-format, serialization, vacuous-test, symmetric-blind-spot, change-tracking, python]
confidence: 9.0
autofix_class: approval_needed
created_utc: "2026-06-02T19:45:00Z"
source_review: "schema-change-tracking-uncommitted-4"
source_issue: "B1"
---

## Problem

A cross-format consistency invariant compared two renderings (`text` vs
`records`) by reducing each to a canonical signature and asserting the
signatures match. The signature function (`_detail_signature`) covered only 1.5
of the 5 detail dimensions a change record can carry: it compared
`applied_to_subclasses` fully, flattened `label_property` to a content-free
boolean (`("label", True)`, discarding the value), and **omitted `order`,
`parent`, and `inherited` entirely**.

The text parser on the other side dropped the same three dimensions. Because the
omission was **symmetric** (both sides discard `order`/`parent`/`inherited`), the
invariant passes *vacuously* on those dimensions — a genuine divergence in
reorder order, declared parent, or inherited count between the two renderers
would NOT fail the test. Reproduced: records `order=[a,b,c]` vs text
`[c,b,a]` produce equal signatures; records `parent=Base/inherited=5` vs text
`WRONGPARENT/+999` produce equal signatures.

This is the most dangerous test failure mode: the one guard the review process
leans on to make "green" meaningful has a blind spot **the same shape as the
defect family it was built to catch** (a value present in one format, absent or
wrong in the other). It also masks a *live* asymmetry: `label_property`'s value
is in `records` but never rendered to `text`, and the boolean-flattened signature
cannot see it.

## Solution

1. **Make the signature total over every published field.** Enumerate the detail
   keys the records format can emit (here: `applied_to_subclasses`,
   `label_property` *value*, `order` as an order-preserving tuple, `parent`,
   `inherited`) and canonicalise all of them. A reorder's whole meaning IS the
   order — compare it as a sequence, not a set.

2. **Make both renderers actually emit every compared field**, or the invariant
   will go red exposing a real divergence. Widening the signature and fixing the
   renderer are COUPLED: rendering `label_property`'s value in text and widening
   the signature must land together, because widening alone turns the live
   `label_property` text/records asymmetry into a (correct) test failure.

3. **Prove non-vacuity.** For every dimension the invariant claims to compare,
   add a test that perturbs that dimension on ONE side and asserts the invariant
   FAILS. A "non-vacuous" claim is only as wide as the dimensions actually
   perturbed in a red test — proving it fails on multiplicity does NOT prove it
   fails on order/parent/inherited content.

4. Prefer single-sourcing the grammar (derive the expected signature from the
   same per-line formatters the renderer uses) so the parser cannot drift into a
   second, divergent encoding of the format.

## When This Applies

- Any test that compares two serializations / formats / encodings of the same
  logical object via a reduced signature or canonical form.
- Any "consistency invariant", "round-trip test", or "cross-format parity" guard.
- Indicator: the signature/canonicalizer drops or flattens fields; both sides of
  the comparison drop the SAME fields (symmetric omission = vacuous pass).
- Strong indicator: the invariant exists specifically to catch a class of
  divergence, but its comparison key excludes the dimension that class lives in.
- Does NOT apply when a field is *intentionally* excluded from comparison (e.g.
  wall-clock timestamps) — but that exclusion must be explicit and justified,
  not an accidental gap, and never the dimension the guard is meant to police.
