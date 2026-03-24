import pytest

from fake_firestore import (
    AsyncFakeCollectionReference,
    AsyncFakeDocumentReference,
    AsyncFakeFirestoreClient,
)
from fake_firestore.async_query import AsyncFakeCollectionGroup


@pytest.fixture
def fs():
    return AsyncFakeFirestoreClient()


@pytest.mark.asyncio
async def test_collection_returns_async(fs):
    coll = fs.collection("foo")
    assert isinstance(coll, AsyncFakeCollectionReference)


@pytest.mark.asyncio
async def test_document_returns_async(fs):
    await fs.collection("foo").document("bar").set({"x": 1})
    doc_ref = fs.document("foo/bar")
    assert isinstance(doc_ref, AsyncFakeDocumentReference)


@pytest.mark.asyncio
async def test_collections(fs):
    await fs.collection("foo").document("a").set({"x": 1})
    await fs.collection("bar").document("b").set({"x": 2})
    colls = [c async for c in fs.collections()]
    ids = {c.id for c in colls}
    assert "foo" in ids
    assert "bar" in ids
    for c in colls:
        assert isinstance(c, AsyncFakeCollectionReference)


@pytest.mark.asyncio
async def test_get_all(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    refs = [
        fs.collection("foo").document("first"),
        fs.collection("foo").document("second"),
    ]
    results = [snap async for snap in fs.get_all(refs)]
    returned_ids = {snap.to_dict()["id"] for snap in results}
    assert returned_ids == {1, 2}


@pytest.mark.asyncio
async def test_collection_group(fs):
    await fs.collection("top").document("d1").set({"x": 1})
    await fs.collection("top").document("d1").collection("sub").document("s1").set({"val": 10})
    await fs.collection("other").document("d2").collection("sub").document("s2").set({"val": 20})

    group = fs.collection_group("sub")
    assert isinstance(group, AsyncFakeCollectionGroup)
    docs = [doc async for doc in group.stream()]
    vals = {doc.to_dict()["val"] for doc in docs}
    assert vals == {10, 20}


@pytest.mark.asyncio
async def test_collection_group_snapshots_preserve_async_reference(fs):
    await fs.collection("top").document("d1").collection("sub").document("s1").set({"val": 10})

    docs = [doc async for doc in fs.collection_group("sub").stream()]

    assert len(docs) == 1
    assert isinstance(docs[0].reference, AsyncFakeDocumentReference)

    await docs[0].reference.update({"updated": True})

    refreshed = await fs.collection("top").document("d1").collection("sub").document("s1").get()
    assert refreshed.to_dict() == {"val": 10, "updated": True}


@pytest.mark.asyncio
async def test_collection_group_with_where(fs):
    await fs.collection("top").document("d1").collection("items").document("i1").set({"score": 10})
    await fs.collection("top").document("d1").collection("items").document("i2").set({"score": 20})
    await (
        fs.collection("other").document("d2").collection("items").document("i3").set({"score": 30})
    )

    group = fs.collection_group("items").where("score", ">=", 20)
    docs = [doc async for doc in group.stream()]
    assert len(docs) == 2
    scores = {doc.to_dict()["score"] for doc in docs}
    assert scores == {20, 30}


@pytest.mark.asyncio
async def test_transaction_returns_async(fs):
    txn = fs.transaction()
    from fake_firestore import AsyncFakeTransaction

    assert isinstance(txn, AsyncFakeTransaction)


@pytest.mark.asyncio
async def test_batch_returns_async(fs):
    batch = fs.batch()
    from fake_firestore import AsyncFakeWriteBatch

    assert isinstance(batch, AsyncFakeWriteBatch)


@pytest.mark.asyncio
async def test_reset(fs):
    await fs.collection("foo").document("bar").set({"x": 1})
    fs.reset()
    doc = await fs.collection("foo").document("bar").get()
    assert doc.exists is False
