import pytest

from fake_firestore import (
    AlreadyExists,
    AsyncFakeCollectionReference,
    AsyncFakeDocumentReference,
    AsyncFakeFirestoreClient,
)
from fake_firestore.async_query import AsyncFakeQuery


@pytest.fixture
def fs():
    return AsyncFakeFirestoreClient()


@pytest.mark.asyncio
async def test_stream_returns_documents(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    docs = [doc async for doc in fs.collection("foo").stream()]
    assert len(docs) == 2
    assert docs[0].to_dict() == {"id": 1}
    assert docs[1].to_dict() == {"id": 2}


@pytest.mark.asyncio
async def test_stream_empty_collection(fs):
    docs = [doc async for doc in fs.collection("foo").stream()]
    assert docs == []


@pytest.mark.asyncio
async def test_stream_orders_by_id(fs):
    await fs.collection("foo").document("beta").set({"id": 1})
    await fs.collection("foo").document("alpha").set({"id": 2})
    docs = [doc async for doc in fs.collection("foo").stream()]
    assert docs[0].to_dict() == {"id": 2}  # alpha comes first


@pytest.mark.asyncio
async def test_add_document(fs):
    doc_content = {"id": "bar", "xy": "z"}
    timestamp, doc_ref = await fs.collection("foo").add(doc_content)
    assert isinstance(doc_ref, AsyncFakeDocumentReference)
    doc = await doc_ref.get()
    assert doc.to_dict() == doc_content


@pytest.mark.asyncio
async def test_add_duplicate_raises(fs):
    doc_content = {"id": "bar"}
    await fs.collection("foo").add(doc_content)
    with pytest.raises(AlreadyExists):
        await fs.collection("foo").add(doc_content)


@pytest.mark.asyncio
async def test_document_returns_async_ref(fs):
    doc_ref = fs.collection("foo").document("bar")
    assert isinstance(doc_ref, AsyncFakeDocumentReference)


@pytest.mark.asyncio
async def test_where_equals(fs):
    await fs.collection("foo").document("first").set({"valid": True})
    await fs.collection("foo").document("second").set({"gumby": False})
    query = fs.collection("foo").where("valid", "==", True)
    assert isinstance(query, AsyncFakeQuery)
    docs = [doc async for doc in query.stream()]
    assert len(docs) == 1
    assert docs[0].to_dict() == {"valid": True}


@pytest.mark.asyncio
async def test_order_by(fs):
    await fs.collection("foo").document("first").set({"order": 2})
    await fs.collection("foo").document("second").set({"order": 1})
    docs = [doc async for doc in fs.collection("foo").order_by("order").stream()]
    assert docs[0].to_dict() == {"order": 1}
    assert docs[1].to_dict() == {"order": 2}


@pytest.mark.asyncio
async def test_limit(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    docs = [doc async for doc in fs.collection("foo").limit(1).stream()]
    assert len(docs) == 1


@pytest.mark.asyncio
async def test_offset(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    await fs.collection("foo").document("third").set({"id": 3})
    docs = [doc async for doc in fs.collection("foo").offset(1).stream()]
    assert len(docs) == 2
    assert docs[0].to_dict() == {"id": 2}


@pytest.mark.asyncio
async def test_start_at(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    await fs.collection("foo").document("third").set({"id": 3})
    docs = [doc async for doc in fs.collection("foo").start_at({"id": 2}).stream()]
    assert len(docs) == 2
    assert docs[0].to_dict() == {"id": 2}


@pytest.mark.asyncio
async def test_end_before(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    await fs.collection("foo").document("third").set({"id": 3})
    docs = [doc async for doc in fs.collection("foo").end_before({"id": 2}).stream()]
    assert len(docs) == 1
    assert docs[0].to_dict() == {"id": 1}


@pytest.mark.asyncio
async def test_list_documents(fs):
    await fs.collection("foo").document("first").set({"order": 2})
    await fs.collection("foo").document("second").set({"order": 1})
    doc_refs = await fs.collection("foo").list_documents()
    assert len(doc_refs) == 2
    for doc_ref in doc_refs:
        assert isinstance(doc_ref, AsyncFakeDocumentReference)


@pytest.mark.asyncio
async def test_nested_collection(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await (
        fs.collection("foo").document("first").collection("bar").document("nested").set({"id": 1.1})
    )
    docs = [doc async for doc in fs.collection("foo").document("first").collection("bar").stream()]
    assert docs[0].to_dict() == {"id": 1.1}


@pytest.mark.asyncio
async def test_collection_id(fs):
    coll = fs.collection("foo")
    assert coll.id == "foo"
    assert isinstance(coll, AsyncFakeCollectionReference)


@pytest.mark.asyncio
async def test_select(fs):
    await fs.collection("foo").document("first").set({"name": "Alice", "age": 30, "city": "Kyiv"})
    docs = [doc async for doc in fs.collection("foo").select(["name", "age"]).stream()]
    assert docs[0].to_dict() == {"name": "Alice", "age": 30}
