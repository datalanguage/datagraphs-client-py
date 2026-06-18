# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.5.9] - 2026-06-18

### Fixed
- HTTP request header values are now sanitised to strip non-Latin-1 characters, preventing encoding errors when header content (e.g. credentials or project names) contains characters outside the Latin-1 range.

## [0.5.8] - 2026-06-08

### Added
- Always-on, in-memory schema change tracking with `Schema.change_report(fmt="text"|"records")` — emits a deterministic, net-effect, semantically-annotated changelog (class/property add/remove/modify, renames, combined rename+modify, subclass creation, reorders, label-property changes) suitable for attaching to a schema migration.
- `change_report(fmt="records")` is the **fully-supported, guaranteed** output: deterministic and complete, with the full `from`/`to`/`fields`/`detail` payload for programmatic consumption. `change_report(fmt="text")` is a **best-effort human-readable** rendering of the same change set — it is not guaranteed to round-trip user-supplied field content (e.g. a `description` containing newlines), and the cross-subclass (`apply_to_subclasses`) annotation in the report is also best-effort; both are documented known limitations. Report cost is approximately `O(L*C)` for cascade-heavy edit histories (`L` cascade ops × `C` annotated subclasses), inherent to annotating each subclass.
- `change_report(fmt="text")` now renders **every** semantic detail dimension the `fmt="records"` output carries — `applied_to_subclasses`, the designated `label_property` **value**, the reorder `order`, and the subclass `parent`/`inherited` count — as JSON-encoded continuation lines, so the default human-readable changelog describes the identical logical change set (including detail content) as the structured records. The JSON encoding makes list/name values (e.g. a class name containing `", "`) round-trip unambiguously.

### Changed
- Change-tracking identity is now resolved by an **event-sourced replay** of the operation log over the baseline (each entity gets a stable identity minted on creation, rebound on rename, ended on deletion) rather than reconstructed post-hoc from rename events. A name that is freed (by rename or delete) and reused (by rename or creation) therefore binds to a *different* identity by construction — so a genuine rename of the original entity is never mislabelled as `added`, and a net-new entity reusing a freed name is never mislabelled as `renamed from <original>`. Identities are held only in memory during reporting and are never written to `to_dict()`/`to_json()` or the wire format.
- `change_report(fmt="records")` no longer emits server-internal top-level metadata keys (e.g. `guid`, `@context`, `@type`) as schema changes — only domain-model metadata (the model name/version) is reported.

### Fixed
- `create_property`/`update_property` with `apply_to_subclasses=True` now cascade transitively to **all** descendant subclasses and preserve the `is_synonym`/`is_filterable` flags correctly. A prior positional-argument bug stopped the cascade after one level and corrupted those flags. **This changes the wire output of `to_dict()`/`to_json()` on `apply_to_subclasses` paths** (deep subclasses now receive the property; the synonym/filterable flags are no longer corrupted); all non-cascade serialisation remains byte-identical to the previous behaviour.
- The `apply_to_subclasses` cascade is now **iterative** (an explicit breadth-first walk over a once-built parent→children index) instead of Python self-recursion: a deep `subClassOf` chain (thousands of levels) no longer raises `RecursionError`, and the per-operation cost is O(descendants) rather than O(classes²) (it no longer re-scans every class once per level).
- **All public mutating methods** are now **atomic (all-or-nothing)** on **any** failure — `create_class`, `create_subclass`, `update_class`, `delete_class`, `assign_label_property`, `assign_label_autogen`, `assign_baseclass`, `assign_class_description`, `create_property`, `update_property`, `rename_property`, `delete_property`, `assign_property_orders`, and `update_schema_metadata` (including the `apply_to_subclasses` cascade). Rollback state is captured at the outermost call boundary — scoped to the operation's footprint (a shallow class-list snapshot for structure plus a property-granular undo journal for the cascade hot path), not a deep copy of the whole schema — and replayed on any exception, so a mid-operation error leaves the schema completely unchanged instead of a partial write. This closes two further partial-write modes that the prior `create_property`/`update_property`-only guarding left open: `create_subclass` whose inherited-property loop raises mid-cascade no longer leaves a half-built subclass (previously reported as `added`), and `assign_label_property` with a non-existent property no longer corrupts `class.labelProperty` to that non-existent name before raising. Because nothing is recorded for a rolled-back op, `change_report()` never surfaces a change for an operation that raised. The rollback restores the class list **in place**, so an externally-held reference (e.g. via `to_dict()`) stays consistent after a rolled-back mutation. (Previously only existence/duplicate/missing-class errors were guarded, up front, and only on `create_property`/`update_property`; other methods and mid-apply errors could leave a partial write with a misleading change report.)
- `change_report()`'s `inherited` count for a `subclass_created` entry now reflects the properties inherited from the parent **at `create_subclass` time** (captured in the operation log), not the subclass's live property count at report time. A property added to a subclass *after* `create_subclass` is therefore no longer silently re-labelled "inherited"; it now surfaces as its own `added` record in both output formats.
- `applied_to_subclasses` is derived from the subclasses the operation actually touched at call time, intersected with the subclasses that still exist at report time — so it neither over-claims subclasses created after the call nor names a subclass another record marks `removed`.
- Two `assign_property_orders` calls on one class collapse to a single net `reordered` (and none when the net order equals baseline), preserving net-effect collapse; an untracked reorder (via `to_dict()`) no longer leaks the internal `__order__` sentinel into either output format.
- `change_report()` is linear (no O(n²)) on the `apply_to_subclasses` path: per-operation subclass resolution is done lazily for surviving ops, and memoised across operations that touch the same subclass set, rather than re-resolving an op-log-length × class-count table.
- `change_report()` degrades gracefully (rather than raising `KeyError`) on name-less class/property dicts introduced by untracked `to_dict()` edits or legacy/API-shaped input.
- Schema mutation is no longer quadratic. The atomic-rollback guard previously deep-copied the **entire** class list on every outermost mutation, so building an N-class schema one mutation at a time was O(N²) and a sequence of L `apply_to_subclasses` cascades at fixed subclass-count C was O(L²·C) (each call re-copying every class, including the properties added by prior calls). Rollback state is now scoped to each operation's footprint — a shallow class-list snapshot plus a property-granular undo journal — making construction O(N) and accumulated wide cascades O(L·C). In practice this took the schema unit-test suite from ~90s back to ~2s; a large cascade build (400 subclasses × 160 cascade properties) dropped from ~19s to under 1s. The atomicity guarantee is unchanged: any mid-operation failure still rolls back byte-for-byte.

## [0.5.7] - 2026-05-27

### Changed
- Resynced `uv.lock`.

## [0.5.6] - 2026-05-27

### Fixed
- Label synonym property is no longer emitted as a non-nullable field in the schema, fixing a nullable-schema validation bug.

## [0.5.5] - 2026-04-28

### Fixed
- `isFilterable` property flag is now preserved correctly on schema updates.

## [0.5.4] - 2026-04-28

### Changed
- Updated the release workflow (`release.yml`).

## [0.5.3] - 2026-04-28

### Fixed
- `isFilterable` property flag is now preserved correctly during schema migrations.

## [0.5.2] - 2026-04-17

### Changed
- `update_schema_metadata` is now public so it can be invoked from automated deployments.

## [0.5.1] - 2026-04-16

### Added
- `Client` constructor now validates that required arguments are supplied.

## [0.5.0] - 2026-04-14

### Added
- Support for GQL/OpenCypher queries.

## [0.4.11] - 2026-04-02

### Changed
- Gateway project deployment now bypasses source/target dataset comparisons for new (first-time) projects.

## [0.4.10] - 2026-03-27

### Changed
- Batch uploading now handles errors per batch, so a single failing batch no longer aborts the entire upload.

## [0.4.9] - 2026-03-25

### Changed
- Added resilience to dataset config loading during project deployment.

## [0.4.8] - 2026-03-25

### Added
- `setup-venv.sh` script to recreate the virtual environment for Linux (Docker) or macOS platforms
- Support for excluded classes in data dump

### Changed
- Integration test credentials now fall back to environment variables (`API_KEY`, `CLIENT_ID`, `CLIENT_SECRET`, `PROJECT_NAME`) when local config file is absent
- CI release workflow injects OAuth credentials via GitHub Secrets for integration tests

## [0.4.7] - 2026-03-23

### Fixed
- Fixed issues in README quick start credentials

### Changed
- Updated release action to run all tests before bumping version and publishing

## [0.4.6] - 2026-03-23

### Added
- Release workflow action which publishes to PyPI
- GitHub Pages API documentation via `pydoctor`

### Changed
- Moved `HTTP` enum into `enums` module
- Switched module-level loggers to private scope
- Refactored schema creation from existing JSON into a static factory method
- Deleted `API.md` and updated README API docs link to point at GitHub Pages
- Tweaked `__init__.py` docs and README

## [0.3.0] - 2026-03-19

### Changed
- Updates to schema name handling and refactored integration test file output locations

## [0.2.9] - 2026-03-17

### Added
- Wait checks for datasets deployment

### Fixed
- Fixed lint issues

## [0.2.8] - 2026-03-15

### Added
- Gateway `load_project()` and `dump_project()` methods
- Integration tests for gateway project load and dump
- `includeDateField` support in gateway interface

### Changed
- Removed schema from `Gateway` constructor
- Standardised naming of `datatype` and `type_name` to `class_name`, renamed `ALL_DATATYPES` to `ALL_CLASSES`
- Refactored data loading
- Updated `load_project()` to use supplied schema rather than default schema
- Updated timeout usage for paginated client requests

## [0.2.7] - 2026-03-13

### Added
- Token-based pagination support in `Client.get()` for large data dumps
- `Gateway.clear_down()` method to clear data from all datasets
- `Gateway.client` property to expose the underlying API client
- Logging to `Client.apply_schema()`, `Client.apply_datasets()`, `Client.clear_dataset()`, and `Client.tear_down()`

### Changed
- Re-added `pyyaml` dependency (bumped to `>=6.0.3`)
- `Gateway.load_data()` now catches and logs errors per-type instead of aborting
- `Gateway.dump_data()` now catches and logs errors per-type instead of aborting
- `Gateway.load_data()` logs an error instead of raising `ValueError` when a class is not found in any dataset
- Missing data file in `_load_from_file` now logs an error instead of raising `FileNotFoundError`

## [0.2.3] - 2026-03-12

### Fixed
- Removed debug `print` statement from `Client.query()`
- Fixed mutable default argument in `Dataset.__init__`
- Fixed wildcard import in `schema` module
- Corrected `Client.query()` return type annotation

### Changed
- Removed unused `pyyaml` dependency
- Added module-level and class/function docstrings throughout
- Improved README with installation, usage, and API documentation
- Added `License :: OSI Approved :: MIT License` classifier
- Exposed `__version__` via `importlib.metadata`

## [0.2.2] - 2026-02-01

### Added
- Initial public beta release
- `Client` for DataGraphs REST API interaction
- `Gateway` for loading/dumping JSON data
- `Schema` for domain model manipulation
- `Dataset` for dataset management
