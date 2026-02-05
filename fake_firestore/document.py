from __future__ import annotations

import operator
from copy import deepcopy
from functools import reduce
from typing import TYPE_CHECKING, Any, Dict, List

from fake_firestore import AlreadyExists, NotFound
from fake_firestore._helpers import (
    Document,
    Store,
    Timestamp,
    delete_by_path,
    get_by_path,
    set_by_path,
)
from fake_firestore._transformations import apply_transformations

if TYPE_CHECKING:
    from fake_firestore.collection import FakeCollectionReference


class FakeDocumentSnapshot:
    def __init__(self, reference: FakeDocumentReference, data: Document) -> None:
        self.reference = reference
        self._doc = deepcopy(data)

    @property
    def id(self) -> str:
        return self.reference.id

    @property
    def exists(self) -> bool:
        return self._doc != {}

    def to_dict(self) -> Document:
        return self._doc

    @property
    def create_time(self) -> Timestamp:
        timestamp = Timestamp.from_now()
        return timestamp

    @property
    def update_time(self) -> Timestamp:
        return self.create_time

    @property
    def read_time(self) -> Timestamp:
        timestamp = Timestamp.from_now()
        return timestamp

    def get(self, field_path: str) -> Any:
        if not self.exists:
            return None
        else:
            return reduce(operator.getitem, field_path.split("."), self._doc)

    def _get_by_field_path(self, field_path: str) -> Any:
        try:
            return self.get(field_path)
        except KeyError:
            return None


class FakeDocumentReference:
    def __init__(self, data: Store, path: List[str], parent: FakeCollectionReference) -> None:
        self._data = data
        self._path = path
        self.parent = parent

    @property
    def id(self) -> str:
        return self._path[-1]

    def get(self) -> FakeDocumentSnapshot:
        try:
            data = get_by_path(self._data, self._path)
        except KeyError:
            data = {}
        return FakeDocumentSnapshot(self, data)

    def create(self, data: Dict[str, Any]) -> None:
        """Create a new document with the given data.

        Raises AlreadyExists if the document already exists.
        """
        try:
            existing = get_by_path(self._data, self._path)
            if existing != {}:
                raise AlreadyExists(f"Document already exists: {self._path}")  # type: ignore[no-untyped-call]
        except KeyError:
            pass
        set_by_path(self._data, self._path, deepcopy(data))

    def delete(self) -> None:
        delete_by_path(self._data, self._path)

    def set(self, data: Dict[str, Any], merge: bool = False) -> None:
        if merge:
            try:
                self.update(deepcopy(data))
            except NotFound:
                self.set(data)
        else:
            set_by_path(self._data, self._path, deepcopy(data))

    def update(self, data: Dict[str, Any]) -> None:
        document = get_by_path(self._data, self._path)
        if document == {}:
            raise NotFound("No document to update: {}".format(self._path))  # type: ignore[no-untyped-call]

        apply_transformations(document, deepcopy(data))

    def collection(self, name: str) -> FakeCollectionReference:
        from fake_firestore.collection import FakeCollectionReference

        document = get_by_path(self._data, self._path)
        new_path = self._path + [name]
        if name not in document:
            set_by_path(self._data, new_path, {})
        return FakeCollectionReference(self._data, new_path, parent=self)


# Backward compatibility aliases
DocumentSnapshot = FakeDocumentSnapshot
DocumentReference = FakeDocumentReference
