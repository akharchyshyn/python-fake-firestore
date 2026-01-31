import pytest

from fake_firestore import MockFirestore, NotFound


def test_contract_get_missing_doc_returns_exists_false(
    fs: MockFirestore, collection_name: str
) -> None:
    doc_ref = fs.collection(collection_name).document("missing")

    snapshot = doc_ref.get()

    assert snapshot.exists is False


def test_contract_set_then_get_returns_same_data(fs: MockFirestore, collection_name: str) -> None:
    doc_ref = fs.collection(collection_name).document("alice")
    payload = {"name": "Alice", "age": 30}

    doc_ref.set(payload)
    snapshot = doc_ref.get()

    assert snapshot.to_dict() == payload


def test_contract_update_missing_doc_raises_not_found(
    fs: MockFirestore, collection_name: str
) -> None:
    doc_ref = fs.collection(collection_name).document("missing")

    with pytest.raises(NotFound):
        doc_ref.update({"active": True})
