# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic
Versioning.

## [0.3.0] - 2026-01-31
### Added
- Add `WriteBatch` support via `client.batch()`.

### Fixed
- Make `DocumentReference.get()` return an empty snapshot for missing documents
  (so `exists` is `False`) instead of raising `KeyError`.

## [0.2.0] - 2026-01-31
### Added
- Implement `collection_group()` for querying across collections.

### Changed
- Rename classes to use the `Fake*` prefix for consistent terminology.
- Rebrand the project and switch to Poetry-based packaging.
- Refresh type annotations and imports for clarity.

