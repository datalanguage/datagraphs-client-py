---
id: sol-011-two-renderers-policed-by-invariant-render-from-one-model
title: "Two independent renderers policed by a comparison invariant will displace divergence forever — render both from one canonical structure"
tags: [serialization, rendering, dual-format, invariant, round-trip, parity, displacement, schema, change-tracking, python]
confidence: 8.5
autofix_class: approval_needed
created_utc: "2026-06-02T20:15:00Z"
source_review: "schema-change-tracking-uncommitted-5"
source_issue: "B1"
---

## Problem

A feature must emit the SAME logical information in two formats (e.g. a
machine-readable `records` form and a human-readable `text` form). Two
independent renderers each walk the source data and produce their format. To
guard against drift, the team adds a cross-format invariant: parse both outputs
back to a canonical comparison key and assert they agree.

This setup makes divergence *relocate* rather than *dissolve*. Across five
review rounds of a schema change-tracking feature, the exact "text describes a
change records doesn't" defect reappeared five times:

1. text drops a second class Change
2. text drops `applied_to_subclasses`; invariant strips detail
3. invariant signature itself vacuous on `order`/`parent`/`inherited`;
   `label_property` value a live text/records asymmetry
4. (same family, signature made total over five `detail` dimensions)
5. invariant total over `detail` only — BLIND to the sibling record members
   `fields`/`from`/`to`, and the text renderer emits user-controlled
   `description` content verbatim and unescaped into a newline-delimited
   grammar, so a description containing a newline injects a phantom field line
   the invariant cannot see.

The structural cause: the invariant is only ever made "total" over the
dimension that was *last* criticised, while staying blind to the next. Each fix
is a band-aid over the real problem — there are TWO independent renderers, and
an ever-growing comparison key is being asked to prove they never disagree on
ANY dimension. That proof obligation grows without bound; every new field is a
fresh place for the two renderers to disagree silently and for the invariant to
miss it. Unescaped free-text content (descriptions, names) is the worst case:
its value-space is unbounded, so no finite signature can canonicalise it.

## Solution

Stop policing two renderers with an invariant. Make one format a pure
*projection* of the other, so divergence is **structurally impossible** rather
than merely tested-against:

- Render both formats from ONE canonical intermediate structure. The text
  renderer should consume the records structure (or a shared typed model), not
  re-walk the source. Then text cannot describe a change records doesn't —
  there is one source of truth and one traversal.
- If a human format must remain, derive it by formatting the canonical records
  (e.g. `text = pretty_print(records)`), never by an independent second walk.
- For any free-text field embedded in a structured grammar, ENCODE it
  (JSON-quote / escape) so its content cannot inject grammar tokens. An
  unescaped value in a delimiter-keyed grammar is unbounded divergence surface
  no signature can cover.

This is the same move that dissolved the sibling "recycle/identity" family in
the same project: attacking the *generating model* (event-sourced identity)
dissolved a family that four rounds of coordinate-level patches only displaced.
Render-from-one-model is the generating-model fix for format divergence.

A retreat option, if single-model rendering is genuinely out of scope: declare
the human format explicitly non-round-trippable and STOP asserting the parity
invariant proves equivalence — but then any "formats agree" NFR is unmet and
must be renegotiated, not silently approximated.

## When This Applies

- Any feature emitting the same data in 2+ formats with a "they must agree" NFR.
- Indicators the trap is present: a comparison/round-trip invariant whose key
  keeps being widened release after release; two separate functions that each
  traverse the source to build a format; free-text/user-controlled values
  emitted verbatim into a delimiter- or prefix-keyed grammar.
- Language-agnostic, but acute in dynamically-typed languages (Python) where no
  compiler forces the two renderers and the parser to stay structurally aligned.
- Does NOT apply when the two formats genuinely carry different information by
  design (then "they must agree" is the wrong NFR — say so explicitly).
