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

    transaction = fs.transaction()
    _maybe_begin(transaction)

    if hasattr(transaction, "__enter__") and hasattr(transaction, "__exit__"):
        with pytest.raises(RuntimeError):
            with transaction:
                transaction.set(doc_ref, {"id": 1})
                raise RuntimeError("boom")
    else:
        with pytest.raises(RuntimeError):
            transaction.set(doc_ref, {"id": 1})
            raise RuntimeError("boom")

    assert doc_ref.get().exists is False
