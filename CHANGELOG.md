# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
