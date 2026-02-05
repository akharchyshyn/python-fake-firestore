import pytest

from fake_firestore import AlreadyExists, MockFirestore, NotFound


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


def test_contract_create_new_doc_succeeds(fs: MockFirestore, collection_name: str) -> None:
    doc_ref = fs.collection(collection_name).document("new_doc")
    payload = {"name": "Bob", "age": 25}

    doc_ref.create(payload)
    snapshot = doc_ref.get()

    assert snapshot.exists is True
    assert snapshot.to_dict() == payload


def test_contract_create_existing_doc_raises_already_exists(
    fs: MockFirestore, collection_name: str
) -> None:
    doc_ref = fs.collection(collection_name).document("existing")
    doc_ref.set({"name": "Alice"})

    with pytest.raises(AlreadyExists):
        doc_ref.create({"name": "Bob"})
