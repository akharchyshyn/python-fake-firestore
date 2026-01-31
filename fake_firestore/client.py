from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Union

from fake_firestore.collection import CollectionReference
from fake_firestore.document import DocumentReference, DocumentSnapshot
from fake_firestore.transaction import Transaction


class MockFirestore:
    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}

    def _ensure_path(
        self, path: List[str]
    ) -> Union[MockFirestore, CollectionReference, DocumentReference]:
        current_position: Union[MockFirestore, CollectionReference, DocumentReference] = self

        for el in path[:-1]:
            if isinstance(current_position, (MockFirestore, DocumentReference)):
                current_position = current_position.collection(el)
            else:
                current_position = current_position.document(el)

        return current_position

    def document(self, path: str) -> DocumentReference:
        path_parts = path.split("/")

        if len(path_parts) % 2 != 0:
            raise Exception("Cannot create document at path {}".format(path_parts))
        current_position = self._ensure_path(path_parts)

        if isinstance(current_position, CollectionReference):
            return current_position.document(path_parts[-1])
        raise Exception("Invalid path")  # pragma: no cover

    def collection(self, path: str) -> CollectionReference:
        path_parts = path.split("/")

        if len(path_parts) % 2 != 1:
            raise Exception("Cannot create collection at path {}".format(path_parts))

        name = path_parts[-1]
        if len(path_parts) > 1:
            current_position = self._ensure_path(path_parts)
            if isinstance(current_position, (MockFirestore, DocumentReference)):
                return current_position.collection(name)
            raise Exception("Invalid path")  # pragma: no cover
        else:
            if name not in self._data:
                self._data[name] = {}
            return CollectionReference(self._data, [name])

    def collections(self) -> Sequence[CollectionReference]:
        return [
            CollectionReference(self._data, [collection_name]) for collection_name in self._data
        ]

    def reset(self) -> None:
        self._data = {}

    def get_all(
        self,
        references: Iterable[DocumentReference],
        field_paths: Optional[Any] = None,
        transaction: Optional[Any] = None,
    ) -> Iterator[DocumentSnapshot]:
        for doc_ref in set(references):
            yield doc_ref.get()

    def transaction(self, **kwargs: Any) -> Transaction:
        return Transaction(self, **kwargs)
