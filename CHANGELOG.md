# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
