from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Union

from fake_firestore._helpers import Timestamp, generate_random_string
from fake_firestore.document import DocumentReference, DocumentSnapshot
from fake_firestore.query import Query

if TYPE_CHECKING:
    from types import TracebackType

    from fake_firestore.client import MockFirestore

MAX_ATTEMPTS = 5
_MISSING_ID_TEMPLATE = "The transaction has no transaction ID, so it cannot be {}."
_CANT_BEGIN = "The transaction has already begun. Current transaction ID: {!r}."
_CANT_ROLLBACK = _MISSING_ID_TEMPLATE.format("rolled back")
_CANT_COMMIT = _MISSING_ID_TEMPLATE.format("committed")


class WriteResult:
    def __init__(self) -> None:
        self.update_time = Timestamp.from_now()


class Transaction:
    """
    This mostly follows the model from
    https://googleapis.dev/python/firestore/latest/transaction.html
    """

    def __init__(
        self,
        client: MockFirestore,
        max_attempts: int = MAX_ATTEMPTS,
        read_only: bool = False,
    ) -> None:
        self._client = client
        self._max_attempts = max_attempts
        self._read_only = read_only
        self._id: Optional[str] = None
        self._write_ops: List[Callable[[], None]] = []
        self.write_results: Optional[List[WriteResult]] = None

    @property
    def in_progress(self) -> bool:
        return self._id is not None

    @property
    def id(self) -> Optional[str]:
        return self._id

    def _begin(self, retry_id: Optional[str] = None) -> None:
        # generate a random ID to set the transaction as in_progress
        self._id = generate_random_string()

    def _clean_up(self) -> None:
        self._write_ops.clear()
        self._id = None

    def _rollback(self) -> None:
        if not self.in_progress:
            raise ValueError(_CANT_ROLLBACK)

        self._clean_up()

    def _commit(self) -> List[WriteResult]:
        if not self.in_progress:
            raise ValueError(_CANT_COMMIT)

        results: List[WriteResult] = []
        for write_op in self._write_ops:
            write_op()
            results.append(WriteResult())
        self.write_results = results
        self._clean_up()
        return results

    def get_all(
        self, references: Iterable[DocumentReference]
    ) -> Iterable[DocumentSnapshot]:
        return self._client.get_all(references)

    def get(
        self, ref_or_query: Union[DocumentReference, Query]
    ) -> Iterable[DocumentSnapshot]:
        if isinstance(ref_or_query, DocumentReference):
            return self._client.get_all([ref_or_query])
        elif isinstance(ref_or_query, Query):
            return ref_or_query.stream()
        else:
            raise ValueError(
                'Value for argument "ref_or_query" must be a DocumentReference or a Query.'
            )

    # methods from
    # https://googleapis.dev/python/firestore/latest/batch.html#google.cloud.firestore_v1.batch.WriteBatch

    def _add_write_op(self, write_op: Callable[[], None]) -> None:
        if self._read_only:
            raise ValueError(
                "Cannot perform write operation in read-only transaction."
            )
        self._write_ops.append(write_op)

    def create(
        self, reference: DocumentReference, document_data: Dict[str, Any]
    ) -> None:
        # this is a no-op, because if we have a DocumentReference
        # it's already in the MockFirestore
        ...

    def set(
        self,
        reference: DocumentReference,
        document_data: Dict[str, Any],
        merge: bool = False,
    ) -> None:
        write_op = partial(reference.set, document_data, merge=merge)
        self._add_write_op(write_op)

    def update(
        self,
        reference: DocumentReference,
        field_updates: Dict[str, Any],
        option: Any = None,
    ) -> None:
        write_op = partial(reference.update, field_updates)
        self._add_write_op(write_op)

    def delete(self, reference: DocumentReference, option: Any = None) -> None:
        write_op = reference.delete
        self._add_write_op(write_op)

    def commit(self) -> List[WriteResult]:
        return self._commit()

    def __enter__(self) -> Transaction:
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if exc_type is None:
            self.commit()
