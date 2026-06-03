---
id: sol-009-redesign-dissolves-root-cause-three-failed-patches
title: "A redesign that attacks the root cause can dissolve a defect family that three rounds of point-fixes only displaced"
tags: [code-review, displacement-pattern, redesign, identity-model, event-sourcing, root-cause, refactoring, change-tracking]
confidence: 9.0
autofix_class: approval_needed
created_utc: "2026-06-02T19:45:00Z"
source_review: "schema-change-tracking-uncommitted-4"
source_issue: "meta-finding"
---

## Problem

A defect "shape" (e.g. recycle/identity-theft, where a freed entity name reused
by a different entity is mis-reported as a rename of the original) recurred for
three consecutive review rounds. Each round a locally-correct point-fix, pinned
by its own bug-first test, closed the instance it was handed — and an equivalent
instance reappeared **one structural layer outward** through a different code
path:

- Recycle/identity: `_diff` (recycle-by-deletion) → flat alias map
  (recycle-by-rename) → `_fold_renames` (recycle-by-creation).
- The tests passed every round; "green" stopped meaning "correct" because each
  test shared the code's blind spot — it reproduced only the instance the fix
  targeted, in the narrowest event stream that instance touched.

The root cause was a single structural property: **identity was reconstructed
post-hoc from one event stream (rename events) rather than minted as a
first-class fact.** "A name is not an identity." Any fix that resolved names
through whichever stream it happened to walk left a recycle through a *different*
op kind unguarded.

## Solution

When a defect displaces across adjacent layers for multiple fix rounds, stop
patching the symptom's current coordinate and **redesign the generating model so
the defect class is impossible by construction**:

- Here: an event-sourced identity model. Every entity gets a stable opaque
  identity **minted on creation, rebound on rename, ended on deletion**. A name
  freed (by rename OR delete) and reused (by rename OR creation) binds a
  *different* identity by construction — so recycle-by-anything cannot mis-resolve.
- The redesign was validated by an adversarial reviewer attacking the dissolved
  family from every angle it had ever appeared (recycle-by-creation,
  rename-then-recreate, three-epoch reuse, untracked-edit collision, diamond /
  multi-level) and **failing to re-break it**. That "could not re-break it" is the
  proof a displacement was *dissolved*, not relocated.
- Critically: a redesign is not automatically a clean slate. Verify each defect
  family independently — in this case two of three families (identity,
  destructive-op-masking) dissolved, while a third (quadratic/NFR) and a
  fourth (format-divergence/invariant-vacuity) *did* displace into the new
  machinery (the cascade, and the invariant's own signature function). Hunt the
  new address of each shape even after a redesign.

The decision rule: three locally-correct point-fixes that each relocate the same
defect are evidence the **generating model** is wrong. A redesign that costs more
than a fourth patch is still cheaper than a fifth, sixth, and seventh.

## When This Applies

- Any review/maintenance context where the SAME class of bug recurs at adjacent
  layers across multiple fix rounds (the "displacement pattern").
- Especially when a "name", "key", or other surface label is being used as a
  stand-in for a true identity, and the bug is identity confusion under reuse.
- Indicator: every fix is correct and tested, yet an equivalent defect reappears
  one layer over. The tests are green because they reproduce only the instance
  just fixed.
- Does NOT apply when the recurrences are genuinely independent bugs, or when a
  single missing guard would close the whole family — reserve the redesign call
  for a demonstrated, repeated structural displacement.
