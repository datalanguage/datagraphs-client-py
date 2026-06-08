---
id: sol-012-prevalidation-is-not-atomicity-cover-every-midapply-raise
title: "Pre-validation is not atomicity unless it covers every mid-apply raise — use snapshot/rollback or build-off-to-the-side"
tags: [atomicity, all-or-nothing, partial-write, pre-validation, rollback, snapshot, mutation, cascade, data-integrity, python]
confidence: 9.5
autofix_class: approval_needed
created_utc: "2026-06-02T20:15:00Z"
source_review: "schema-change-tracking-uncommitted-5"
source_issue: "B2"
---

## Problem

To make a multi-target mutation "all-or-nothing", a common pattern is to add a
pre-validation phase: loop over all targets, check them, then loop again to
apply. The claim becomes "we validate ALL targets before mutating ANY, so a
mid-cascade failure can never leave a partial write."

This is a false sense of atomicity whenever the *apply* loop can itself raise on
a condition pre-validation did not check. In a schema cascade
(`create_property`/`update_property` with `apply_to_subclasses=True`):

- Pre-validation checked only existence and duplicate/presence.
- The apply loop still raised `InvalidInversePropertyError` /
  missing-range `ClassNotFoundError` / datatype / enum errors — *per target*,
  *after* earlier targets were already mutated.
- Worse, each target's property dict was `append`-ed to the class BEFORE the
  `_assign_*` chain ran, so even the FIRST failing target was left half-built.
- And because the op was `_record`-ed only at the END, a raise mid-apply left
  the schema mutated but the change-log empty — `change_report()` then *lied*:
  it diffed live-vs-baseline and reported a "successful add" for an operation
  the caller saw raise. Silent data corruption WITH a lying audit trail — the
  most dangerous category.

The aggravating factor here was documentation: the fix's docstrings, comments,
and tests all asserted the all-or-nothing invariant, but the tests only
exercised the two pre-validated exceptions. A documented-but-false invariant on
a mutation path is worse than documenting nothing — it sends the 2 AM
debugger looking everywhere except the apply loop.

## Solution

Pre-validation grants atomicity ONLY if you can prove the apply loop is
raise-free given successful pre-validation. Two robust mechanisms:

1. **Build off to the side, then commit.** Construct each target's fully-formed
   object on a throwaway value, running the entire validation/assignment chain
   (datatype, inverse_of, enum) so any raise happens BEFORE any shared-state
   mutation. Validate all targets this way, then commit by attaching the
   finished objects. Move every `append`/insert to AFTER the last
   possibly-raising step.

2. **Snapshot / rollback.** Snapshot the affected state (deep-copy the touched
   collections, or the whole aggregate) before the apply loop; restore on any
   exception, then re-raise. A re-entrancy depth counter is NOT a snapshot —
   verify the "transaction" primitive actually restores state.

Either way:
- Ensure the operation is *recorded* atomically with the mutation (record on
  success only, or roll back the record too) so the audit trail can never claim
  a mutation that raised.
- Add a bug-first test that drives a mid-apply raise (the one pre-validation
  does NOT cover) and asserts the aggregate is byte-unchanged AND no
  side-record was emitted.
- Make comments/docstrings state the EXACT atomicity guarantee ("atomic for
  existence/duplicate failures; the apply loop can still partial-write on
  inverse/datatype errors") until the real fix lands. Never over-promise an
  invariant the code doesn't hold.

## When This Applies

- Any multi-step or multi-target mutation claiming all-or-nothing / transactional
  behaviour via a pre-validate-then-apply structure.
- Indicators the trap is present: pre-validation that checks only a subset of
  the conditions the apply phase can raise on; mutation/append performed before
  the validating assignment that can raise; a success-record written at the end
  of the apply loop; docstrings/tests asserting atomicity but only exercising
  the pre-validated failure modes; no snapshot/rollback wrapping the apply.
- Especially dangerous when a separate read path (a report, an audit log, a
  cache) derives state by diffing live-vs-baseline rather than from the recorded
  op — a partial write then produces a *lying* derived view.
- Language-agnostic. Most acute where exceptions can originate deep in a call
  chain the apply-loop author did not enumerate.
