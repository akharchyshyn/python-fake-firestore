# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic
Versioning.

## [0.6.0] - 2026-02-07
### Added
- Add `transactional` decorator for running functions inside a transaction with
  automatic begin, commit, and retry on failure.
  Closes mdowds/python-mock-firestore#50.
- Add `transaction` keyword argument to `DocumentReference.get()`.
- Transaction context manager now calls `_begin()` / `_rollback()` automatically.

- Add `select()` method to `CollectionReference`, `Query`, and
  `CollectionGroup` for field projection.
  Closes mdowds/python-mock-firestore#52.

### Fixed
- `Collection.get()` and `Query.get()` now return a `list` instead of a
  generator, matching real Firestore behavior.
  Closes mdowds/python-mock-firestore#51.

## [0.5.0] - 2026-02-07
### Fixed
- `document()` and `collection()` no longer create phantom empty entries in the
  store when the referenced document doesn't exist.
- Empty documents (`set({})` / `create({})`) are now correctly treated as
  existing (`exists == True`).
- `stream()` no longer yields non-existing documents.
- `update()` on an existing empty document no longer raises `NotFound`.
- `to_dict()` now returns `None` for non-existing documents instead of `{}`.

## [0.4.0] - 2026-02-01
### Added
- Add `DocumentReference.create()` method that raises `AlreadyExists` if the
  document already exists.
- Add contract tests and Firestore emulator runner.

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

