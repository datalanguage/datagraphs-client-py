---
id: sol-007-consistency-invariant-blind-to-stripped-dimension
title: "A consistency invariant that strips a dimension is blind to divergence in that dimension (and to shared-upstream corruption)"
tags: [testing, invariant, cross-format, consistency-check, false-confidence, python, serialization, audit-log]
confidence: 8.5
autofix_class: approval_needed
created_utc: "2026-06-02T17:30:00Z"
source_review: "schema-change-tracking-uncommitted-3"
source_issue: "intent-B1 / assumptions-B1 / consequences-N1"
---

## Problem

A "consistency invariant" — a test that asserts two representations of the same
data agree (format A vs format B, serialise vs deserialise, two renderers of one
model) — is only as strong as the dimensions it actually compares. When the
invariant **canonicalises away** a dimension before comparing (strips `detail`,
collapses to a `set` discarding multiplicity, compares keys but not values), it
is **structurally unable** to detect a divergence that lives only in the stripped
dimension. A green invariant then reads as "the formats agree" when they do not.

Two distinct failure modes, both observed in one codebase:

1. **Stripped-dimension blindness.** A cross-format invariant compared only
   `(target, op)` pairs and stripped the `detail` field. A deliverable
   (`applied_to_subclasses`) that was present in the structured format but
   *absent* from the default text format passed the invariant, because the
   divergence was entirely inside `detail`. The exact failure shape the invariant
   was built to catch (a deliverable visible in one format, absent in the other)
   survived *inside the device built to police it.*

2. **Shared-upstream vacuous pass.** Both representations were rendered from one
   shared intermediate structure. When that structure was corrupted upstream
   (a poisoned identity map), both renderers produced the *same wrong answer*, so
   the invariant compared wrong-against-identical-wrong and **passed vacuously**.
   The invariant guards *renderer divergence*, never *upstream correctness* — yet
   a passing run was being read as evidence of correctness.

## Solution

- **Compare the dimension where the next bug will live.** If `detail`/`value`/
  ordering/multiplicity carries meaning, the invariant must include it. Compare a
  canonicalised `(target, op, sorted-detail, from, to)` tuple, and use a
  `collections.Counter` rather than a `set` when multiplicity is semantic.
- **Name what the invariant does and does NOT guard.** State in the docstring:
  "this guards renderer/format divergence at `(target, op)` granularity; it does
  NOT guard content parity or upstream identity correctness." Then keep *separate*
  identity-correctness tests that assert concrete values, so a shared-upstream
  corruption (which the parity invariant cannot see) is caught somewhere.
- **Beware the green-light-over-a-hole.** A passing consistency check is evidence
  the compared dimensions agree — nothing more. Do not let it stand in for
  correctness of the data both sides render.
- **Watch the coupling cost of hardening.** Adding a stripped dimension back into
  the comparison will turn currently-green call sites RED if a real divergence
  exists (that is the point) — so hardening the invariant is coupled to fixing the
  divergence it newly exposes; sequence them together, not the checker alone.

## When This Applies

- Any test asserting two serialisations / renderings / round-trips of one model
  agree (text vs JSON, API vs DB, encode/decode round-trip, two output formats).
- Especially when the comparison normalises/canonicalises before asserting
  (`set()`, sorting, dropping fields, lowercasing) — each normalisation is a
  blind spot.
- Especially when both sides derive from one shared intermediate — then the
  invariant cannot see any bug upstream of the split, and will pass vacuously on
  shared corruption.
- Audit logs, changelogs, migration-safety reports — anywhere "the two views
  agree" is being trusted as "the view is correct."
- Does NOT apply when the two sides are derived fully independently from source
  AND the comparison is over the complete semantic content (then a pass is
  genuinely strong).
