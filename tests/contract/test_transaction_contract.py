import os
from typing import Any, Callable, Optional

import pytest

from fake_firestore import MockFirestore


def _maybe_begin(transaction: Any) -> None:
    begin: Optional[Callable[[], None]] = getattr(transaction, "_begin", None)
    if callable(begin):
        begin()


def test_contract_transaction_set_update_delete_applies(
    fs: MockFirestore, collection_name: str
) -> None:
    doc_set = fs.collection(collection_name).document("set_doc")
    doc_update = fs.collection(collection_name).document("update_doc")
    doc_delete = fs.collection(collection_name).document("delete_doc")

    doc_update.set({"count": 1})
    doc_delete.set({"gone": True})

    transaction = fs.transaction()
    _maybe_begin(transaction)
    transaction.set(doc_set, {"id": 1})
    transaction.update(doc_update, {"count": 2})
    transaction.delete(doc_delete)
    transaction.commit()

    assert doc_set.get().to_dict() == {"id": 1}
    assert doc_update.get().to_dict() == {"count": 2}
    assert doc_delete.get().exists is False


def test_contract_transaction_exception_does_not_apply(
    fs: MockFirestore, collection_name: str
) -> None:
    doc_ref = fs.collection(collection_name).document("boom")

    with pytest.raises(RuntimeError):
        with fs.transaction() as transaction:
            transaction.set(doc_ref, {"id": 1})
            raise RuntimeError("boom")

    assert doc_ref.get().exists is False


def _get_transactional() -> Any:
    backend = os.getenv("FIRESTORE_BACKEND", "fake")
    if backend == "emulator":
        from google.cloud import firestore

        return firestore.transactional
    else:
        from fake_firestore import transactional

        return transactional


def test_contract_transactional_decorator_commits(fs: MockFirestore, collection_name: str) -> None:
    _transactional = _get_transactional()
    doc_ref = fs.collection(collection_name).document("counter")
    doc_ref.set({"count": 0})

    @_transactional
    def increment(transaction: Any, ref: Any) -> None:
        snapshot = ref.get(transaction=transaction)
        transaction.update(ref, {"count": snapshot.get("count") + 1})

    transaction = fs.transaction()
    increment(transaction, doc_ref)

    assert doc_ref.get().to_dict() == {"count": 1}


def test_contract_transactional_decorator_rolls_back_on_error(
    fs: MockFirestore, collection_name: str
) -> None:
    _transactional = _get_transactional()
    doc_ref = fs.collection(collection_name).document("safe")
    doc_ref.set({"value": "original"})

    @_transactional
    def failing_update(transaction: Any, ref: Any) -> None:
        transaction.update(ref, {"value": "changed"})
        raise RuntimeError("something went wrong")

    transaction = fs.transaction()
    with pytest.raises(RuntimeError):
        failing_update(transaction, doc_ref)

    assert doc_ref.get().to_dict() == {"value": "original"}
