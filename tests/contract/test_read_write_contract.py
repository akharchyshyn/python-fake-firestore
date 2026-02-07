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


def test_contract_get_missing_doc_does_not_create_it(
    fs: MockFirestore, collection_name: str
) -> None:
    """Getting a non-existing document should not create a phantom entry in the collection."""
    assert fs.collection(collection_name).document("bar").get().exists is False
    assert len(list(fs.collection(collection_name).stream())) == 0


def test_contract_set_empty_doc_exists(fs: MockFirestore, collection_name: str) -> None:
    """A document created with set({}) should be treated as existing."""
    fs.collection(collection_name).document("empty").set({})
    snapshot = fs.collection(collection_name).document("empty").get()
    assert snapshot.exists is True


def test_contract_set_empty_doc_appears_in_stream(fs: MockFirestore, collection_name: str) -> None:
    """An empty document should appear in stream() results."""
    fs.collection(collection_name).document("empty").set({})
    docs = list(fs.collection(collection_name).stream())
    assert len(docs) == 1
    assert docs[0].id == "empty"


def test_contract_create_empty_doc_exists(fs: MockFirestore, collection_name: str) -> None:
    """A document created with create({}) should be treated as existing."""
    fs.collection(collection_name).document("empty").create({})
    snapshot = fs.collection(collection_name).document("empty").get()
    assert snapshot.exists is True


def test_contract_stream_only_returns_existing_docs(
    fs: MockFirestore, collection_name: str
) -> None:
    """stream() should only return documents that actually exist."""
    fs.collection(collection_name).document("real").set({"x": 1})
    # Just get a reference and read a missing doc — should not pollute stream
    fs.collection(collection_name).document("ghost").get()
    docs = list(fs.collection(collection_name).stream())
    assert len(docs) == 1
    assert docs[0].id == "real"


def test_contract_update_empty_doc_succeeds(fs: MockFirestore, collection_name: str) -> None:
    """Updating an existing empty document should work, not raise NotFound."""
    fs.collection(collection_name).document("empty").set({})
    fs.collection(collection_name).document("empty").update({"key": "value"})
    snapshot = fs.collection(collection_name).document("empty").get()
    assert snapshot.to_dict() == {"key": "value"}


def test_contract_create_after_get_missing_succeeds(
    fs: MockFirestore, collection_name: str
) -> None:
    """create() should succeed after a get() on a missing doc (no phantom)."""
    doc_ref = fs.collection(collection_name).document("doc")
    doc_ref.get()  # should not create the doc
    doc_ref.create({"name": "Alice"})
    assert doc_ref.get().exists is True


def test_contract_collection_get_returns_list(fs: MockFirestore, collection_name: str) -> None:
    """collection.get() should return a list, not a generator."""
    fs.collection(collection_name).document("a").set({"x": 1})
    fs.collection(collection_name).document("b").set({"x": 2})

    result = fs.collection(collection_name).get()
    assert isinstance(result, list)
    assert len(result) == 2


def test_contract_snapshot_get_missing_field_raises_key_error(
    fs: MockFirestore, collection_name: str
) -> None:
    """snapshot.get() for a non-existing field should raise KeyError."""
    fs.collection(collection_name).document("doc").set({"name": "Alice"})
    snapshot = fs.collection(collection_name).document("doc").get()
    with pytest.raises(KeyError):
        snapshot.get("missing_field")


def test_contract_snapshot_get_with_subcollection_no_data(
    fs: MockFirestore, collection_name: str
) -> None:
    """A document with only subcollections (no own data) should return None for field access."""
    doc_ref = fs.collection(collection_name).document("parent")
    doc_ref.collection("items").document("item1").set({"name": "My Item"})

    snapshot = doc_ref.get()
    assert snapshot.get("name") is None
