# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
