---
description: Draft CHANGELOG.md entries under [Unreleased] from the commits since the last release
argument-hint: "[optional: base ref to diff from, e.g. v0.6.4]"
allowed-tools: Bash(git log:*), Bash(git diff:*), Bash(git describe:*), Bash(git tag:*), Bash(git status:*), Read, Edit
---

Draft Keep a Changelog entries for the work done since the last release and write
them under `## [Unreleased]` in `CHANGELOG.md`.

This runs **before** cutting a release. It does not bump versions, promote the
`[Unreleased]` heading, commit, or tag — the release workflow owns all of that.

## Procedure

1. **Establish the base ref.**
   - If `$ARGUMENTS` is non-empty, use it as the base.
   - Otherwise resolve the last release tag:
     `git describe --tags --abbrev=0 --match 'v*' 2>/dev/null`
   - If no tag is reachable (tags are created by CI and may not be fetched
     locally), fall back to the most recent release commit:
     `git log --format='%H %s' | grep -m1 'release: v'`
   - State the base ref you settled on and how you found it.

2. **Read what changed.** Both of these — the log alone is not enough to write
   entries at the right level:
   - `git log <base>..HEAD --format='%h %s%n%b'`
   - `git diff <base>..HEAD -- . ':(exclude)uv.lock' ':(exclude)docs/_build'`

   If the diff is large, read it in slices rather than summarising from the log.

3. **Read the existing `CHANGELOG.md`** — at minimum the two most recent released
   sections. Match their voice and depth. Note anything already sitting under
   `## [Unreleased]`; you are merging into it, not replacing it.

4. **Draft the entries**, grouped under the Keep a Changelog headings that
   actually apply: `### Added`, `### Changed`, `### Fixed`, `### Removed`,
   `### Deprecated`, `### Security`. Omit headings with no content.

5. **Write them under `## [Unreleased]`**, merging with any existing entries
   there (fold duplicates together rather than repeating them).

6. **Stop and report.** Show `git diff -- CHANGELOG.md` and ask the user to
   review. Do not commit.

## Style

The existing changelog is written at the level of **behavioural contract**, not
code motion. Match it:

- Describe what a *user of the library* can now do, must now do differently, or
  no longer suffers from. Name the public API surface involved
  (`Schema.change_report`, `create_property(apply_to_subclasses=True)`).
- For fixes, say what was wrong and what the wrong behaviour caused — not just
  "fixed a bug in X".
- Call out anything that changes serialised output, wire format, or complexity
  characteristics. Previous entries flag these explicitly in bold; do the same.
- State performance changes with their complexity class and, where the diff or
  commit messages give real figures, the measured effect.
- One entry per coherent change, not one per commit. Several commits fixing one
  thing are one entry; a commit doing three things is three entries.

## Guardrails

- **Never invent an entry.** Every line must be traceable to the diff. If a
  commit message claims something the diff does not show, trust the diff and say
  so in your report.
- **Omit changes with no user-visible effect** — lockfile resyncs, CI config,
  test-only changes, formatting, internal refactors that preserve behaviour.
  If that leaves nothing at all, say so and leave `[Unreleased]` untouched
  rather than padding it.
- **Do not touch** the `## [Unreleased]` heading itself, any released section,
  version numbers, or dates.
- If the working tree has uncommitted changes, mention them — they are not part
  of the release and are probably not yours to describe.
