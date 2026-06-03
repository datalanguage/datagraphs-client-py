---
id: sol-008-recurring-bug-three-layers-needs-redesign
title: "When a bug recurs at adjacent layers across fix rounds (displacement), the point-fix relocates the defect — redesign the model, do not patch again"
tags: [code-review, architecture, debugging, point-fix, displacement, root-cause, refactoring, regression]
confidence: 8.0
autofix_class: approval_needed
created_utc: "2026-06-02T17:30:00Z"
source_review: "schema-change-tracking-uncommitted-3"
source_issue: "displacement-meta-finding (assumptions-B1 / performance-B1 / intent-B1)"
---

## Problem

A blocking bug is fixed, the fix is locally correct and pinned by a bug-first
test — and the *same defect-shape* reappears one structural layer outward in the
next review. Fix that, and it reappears again one layer further out. This is
**displacement**: the point-fix does not remove the defect, it relocates it,
because the fix addresses the symptom at its observed coordinate rather than the
structural property that generates symptoms at every coordinate.

Observed across three review rounds on one subsystem:

| Defect class | Round 1 | Round 2 | Round 3 |
|---|---|---|---|
| Identity / recycled name | `_diff` (recycle-by-deletion) | flat alias map (recycle-by-rename) | `_fold_renames` (recycle-by-creation) |
| Quadratic / NFR | `_annotate` scan | per-op helper scan | eager L×C table in the fold |
| Format divergence | latent | renderer drops 2nd change | renderer drops a detail; invariant strips detail |

Tell-tale signs you are displacing rather than fixing:

- The "fix" closes the *specific* instance the test reproduces but a sibling
  instance (same defect through a different code path / op kind / input shape)
  is left unguarded.
- Multiple independent reviewers find different faces of the *same* root cause in
  one round without coordinating.
- Each fix's mechanism becomes the next round's bug site (the alias map that
  fixed chains collided on recycle; the op-time capture that fixed over-claim
  rebuilt the quadratic).

## Solution

- **Recognise displacement as a redesign signal, not a patch backlog.** After a
  defect-shape recurs ~2-3 times at adjacent layers, stop patching. The cost of
  the next point-fix is not one fix — it is the guaranteed re-emergence.
- **Name the generating property, then fix THAT.** Ask "what structural property
  makes this defect possible at every layer?" Examples from the source review:
  identity resolved through whichever event stream a fix happened to walk (so any
  *other* event kind slips the guard) — fix: a single creation-aware identity
  fold that registers every entity's identity regardless of how it was
  introduced. Faithful per-op state capture in tension with an O(n) NFR — fix:
  resolve lazily/bounded for surviving items, never materialise the full product.
- **Test the invariant, not the instance.** Replace per-instance regression tests
  with a property/parametrised test that exercises the defect across all paths
  (every op kind, both formats, concrete values) so a displaced sibling cannot
  pass.
- **Distinguish "fix is correct" from "constraint should hold."** A correct fix
  can legitimately break a stated constraint (e.g. byte-identical output) when the
  prior behaviour was itself buggy — record that as a deliberate, documented
  decision rather than reverting or guarding against the now-false constraint.

## When This Applies

- Any multi-round code review or bug-fix cycle where "fixed" findings keep
  reappearing in a slightly different location or through a different input path.
- Subsystems with two truth sources, multiple coordinate systems, or a
  structural tension between two requirements (e.g. fidelity vs performance) —
  point-fixes oscillate between satisfying one and violating the other.
- When independent reviewers converge on one root cause from different angles in
  a single round.
- Does NOT apply to genuinely distinct, unrelated bugs that happen to land in one
  file, or to the first occurrence of a defect (one data point is not a
  trajectory).
