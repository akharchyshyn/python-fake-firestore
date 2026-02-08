# Changelog
All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic
Versioning.

## [0.11.2] - 2026-02-08
### Fixed
- Add `__eq__` and `__hash__` to `DocumentReference` so that references with
  the same path compare equal. Fixes querying by reference fields.
  Closes mdowds/python-mock-firestore#68.

## [0.11.1] - 2026-02-08
### Fixed
- Remove incorrect deprecation warnings from `Collection.get()` and
  `Query.get()`. These methods are not deprecated in the real Firestore SDK.

## [0.11.0] - 2026-02-08
### Added
- Async support: `AsyncFakeFirestoreClient`, `AsyncFakeDocumentReference`,
  `AsyncFakeCollectionReference`, `AsyncFakeQuery`, `AsyncFakeCollectionGroup`,
  `AsyncFakeTransaction`, `AsyncFakeWriteBatch`, and `async_transactional`
  decorator for testing code that uses `google.cloud.firestore_v1.async_*`
  classes.

## [0.10.1] - 2026-02-08
### Fixed
- `DocumentReference.get()` now supports optional `field_paths`, `retry`, and
  `timeout` arguments, matching the real Firestore API.
  Closes mdowds/python-mock-firestore#86.

## [0.10.0] - 2026-02-08
### Added
- `DocumentReference.collections()` method to list subcollections.
  Closes mdowds/python-mock-firestore#85.
- `CollectionReference.id` property.

## [0.9.0] - 2026-02-08
### Added
- `where()` now accepts `filter=FieldFilter(...)` keyword argument, matching
  the modern Firestore API. Closes mdowds/python-mock-firestore#74.

## [0.8.3] - 2026-02-08
### Fixed
- Transforms (`Increment`, `ArrayUnion`) on non-existent documents no longer
  store raw transform objects instead of resolved values.
  Closes mdowds/python-mock-firestore#71.

## [0.8.2] - 2026-02-08
### Fixed
- `array_contains` and `array_contains_any` queries no longer throw `TypeError`
  when the field is `None` or missing. Closes mdowds/python-mock-firestore#66.

## [0.8.1] - 2026-02-08
### Fixed
- `ArrayUnion` now skips elements already present in the array, matching real
  Firestore behavior. Closes mdowds/python-mock-firestore#58.

## [0.8.0] - 2026-02-07
### Fixed
- Documents with only subcollections (no own data) no longer incorrectly
  appear as existing. Introduced `_written_docs` tracking to distinguish
  explicitly written documents from intermediate paths created by
  subcollections. Closes mdowds/python-mock-firestore#57.

### Changed
- Rewrite all unit tests to use the public API (`set()`, `create()`) instead
  of setting `fs._data` directly.

## [0.7.1] - 2026-02-07
### Fixed
- Resolve mypy error in `_apply_projection` with fallback for projection type.

## [0.7.0] - 2026-02-07
### Added
- Add `select()` method to `CollectionReference`, `Query`, and
  `CollectionGroup` for field projection.
  Closes mdowds/python-mock-firestore#52.

### Fixed
- `Collection.get()` and `Query.get()` now return a `list` instead of a
  generator, matching real Firestore behavior.
  Closes mdowds/python-mock-firestore#51.

## [0.6.0] - 2026-02-07
### Added
- Add `transactional` decorator for running functions inside a transaction with
  automatic begin, commit, and retry on failure.
  Closes mdowds/python-mock-firestore#50.
- Add `transaction` keyword argument to `DocumentReference.get()`.
- Transaction context manager now calls `_begin()` / `_rollback()` automatically.

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

