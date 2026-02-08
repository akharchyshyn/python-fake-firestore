import pytest

from fake_firestore import (
    AlreadyExists,
    AsyncFakeCollectionReference,
    AsyncFakeDocumentReference,
    AsyncFakeFirestoreClient,
    NotFound,
)


@pytest.fixture
def fs():
    return AsyncFakeFirestoreClient()


@pytest.mark.asyncio
async def test_get_document(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"id": 1}
    assert doc.id == "first"


@pytest.mark.asyncio
async def test_get_document_by_path(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    doc = await fs.document("foo/first").get()
    assert doc.to_dict() == {"id": 1}


@pytest.mark.asyncio
async def test_get_document_does_not_exist(fs):
    doc = await fs.collection("foo").document("bar").get()
    assert doc.to_dict() is None
    assert doc.exists is False


@pytest.mark.asyncio
async def test_get_document_new_returns_default_id(fs):
    doc_ref = fs.collection("foo").document()
    doc = await doc_ref.get()
    assert doc_ref.id is not None
    assert doc.exists is False


@pytest.mark.asyncio
async def test_set_document(fs):
    doc_content = {"id": "bar"}
    await fs.collection("foo").document("bar").set(doc_content)
    doc = await fs.collection("foo").document("bar").get()
    assert doc.to_dict() == doc_content


@pytest.mark.asyncio
async def test_set_merge(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("first").set({"updated": True}, merge=True)
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"id": 1, "updated": True}


@pytest.mark.asyncio
async def test_set_merge_nonexistent(fs):
    await fs.collection("foo").document("first").set({"updated": True}, merge=True)
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"updated": True}


@pytest.mark.asyncio
async def test_set_overwrite(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("first").set({"new_id": 1}, merge=False)
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"new_id": 1}


@pytest.mark.asyncio
async def test_update_add_new_value(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("first").update({"updated": True})
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"id": 1, "updated": True}


@pytest.mark.asyncio
async def test_update_nonexistent_raises(fs):
    with pytest.raises(NotFound):
        await fs.collection("foo").document("nonexistent").update({"id": 2})


@pytest.mark.asyncio
async def test_delete_document(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("first").delete()
    doc = await fs.collection("foo").document("first").get()
    assert doc.exists is False


@pytest.mark.asyncio
async def test_create_document(fs):
    doc_content = {"id": "bar"}
    await fs.collection("foo").document("bar").create(doc_content)
    doc = await fs.collection("foo").document("bar").get()
    assert doc.to_dict() == doc_content


@pytest.mark.asyncio
async def test_create_raises_already_exists(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    with pytest.raises(AlreadyExists):
        await fs.collection("foo").document("first").create({"id": 2})
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"id": 1}


@pytest.mark.asyncio
async def test_collection_returns_async_ref(fs):
    doc_ref = fs.collection("foo").document("parent")
    subcoll = doc_ref.collection("sub")
    assert isinstance(subcoll, AsyncFakeCollectionReference)


@pytest.mark.asyncio
async def test_collections_returns_async_refs(fs):
    doc_ref = fs.collection("foo").document("parent")
    await doc_ref.collection("sub_a").document("child1").set({"x": 1})
    await doc_ref.collection("sub_b").document("child2").set({"x": 2})

    subcollections = await doc_ref.collections()
    ids = {col.id for col in subcollections}
    assert ids == {"sub_a", "sub_b"}
    for col in subcollections:
        assert isinstance(col, AsyncFakeCollectionReference)


@pytest.mark.asyncio
async def test_collections_empty(fs):
    doc_ref = fs.collection("foo").document("leaf")
    await doc_ref.set({"name": "leaf"})
    subcollections = await doc_ref.collections()
    assert subcollections == []


@pytest.mark.asyncio
async def test_document_returns_async_ref(fs):
    doc_ref = fs.collection("foo").document("bar")
    assert isinstance(doc_ref, AsyncFakeDocumentReference)


@pytest.mark.asyncio
async def test_get_with_field_paths(fs):
    await fs.collection("foo").document("doc").set({"name": "Alice", "age": 30, "city": "NYC"})
    snapshot = await fs.collection("foo").document("doc").get(field_paths=["name", "city"])
    assert snapshot.exists
    assert snapshot.to_dict() == {"name": "Alice", "city": "NYC"}


@pytest.mark.asyncio
async def test_get_nested_document(fs):
    await fs.collection("top").document("doc").set({"id": 1})
    await (
        fs.collection("top").document("doc").collection("nested").document("child").set({"id": 1.1})
    )
    doc = await fs.collection("top").document("doc").collection("nested").document("child").get()
    assert doc.to_dict() == {"id": 1.1}
