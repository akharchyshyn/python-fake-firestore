import pytest

from fake_firestore import AsyncFakeFirestoreClient


@pytest.fixture
def fs():
    client = AsyncFakeFirestoreClient()
    return client


@pytest.fixture
async def populated_fs(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})
    return fs


@pytest.mark.asyncio
async def test_transaction_aenter_aexit(populated_fs):
    fs = populated_fs
    async with fs.transaction() as txn:
        txn._begin()
        doc = fs.collection("foo").document("first")
        txn.set(doc, {"id": 10})
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"id": 10}


@pytest.mark.asyncio
async def test_transaction_commit(populated_fs):
    fs = populated_fs
    txn = fs.transaction()
    txn._begin()
    doc_ref = fs.collection("foo").document("third")
    txn.set(doc_ref, {"id": 3})
    results = await txn.commit()
    assert len(results) == 1
    doc = await doc_ref.get()
    assert doc.to_dict() == {"id": 3}


@pytest.mark.asyncio
async def test_transaction_get_document(populated_fs):
    fs = populated_fs
    async with fs.transaction() as txn:
        txn._begin()
        doc = fs.collection("foo").document("first")
        results = [snap async for snap in txn.get(doc)]
        assert len(results) == 1
        assert results[0].to_dict() == {"id": 1}


@pytest.mark.asyncio
async def test_transaction_get_all(populated_fs):
    fs = populated_fs
    async with fs.transaction() as txn:
        txn._begin()
        refs = [
            fs.collection("foo").document("first"),
            fs.collection("foo").document("second"),
        ]
        results = [snap async for snap in txn.get_all(refs)]
        returned = {snap.to_dict()["id"] for snap in results}
        assert returned == {1, 2}


@pytest.mark.asyncio
async def test_transaction_get_query(populated_fs):
    fs = populated_fs
    async with fs.transaction() as txn:
        txn._begin()
        query = fs.collection("foo").order_by("id")
        results = [snap async for snap in txn.get(query)]
        assert len(results) == 2


@pytest.mark.asyncio
async def test_transaction_update(populated_fs):
    fs = populated_fs
    async with fs.transaction() as txn:
        txn._begin()
        doc = fs.collection("foo").document("first")
        txn.update(doc, {"updated": True})
    doc = await fs.collection("foo").document("first").get()
    assert doc.to_dict() == {"id": 1, "updated": True}


@pytest.mark.asyncio
async def test_transaction_delete(populated_fs):
    fs = populated_fs
    async with fs.transaction() as txn:
        txn._begin()
        doc = fs.collection("foo").document("first")
        txn.delete(doc)
    doc = await fs.collection("foo").document("first").get()
    assert doc.exists is False


@pytest.mark.asyncio
async def test_write_batch_commit(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    batch = fs.batch()
    doc_ref = fs.collection("foo").document("second")
    batch.set(doc_ref, {"id": 2})
    results = await batch.commit()
    assert len(results) == 1
    doc = await doc_ref.get()
    assert doc.to_dict() == {"id": 2}


@pytest.mark.asyncio
async def test_write_batch_aenter_aexit(fs):
    doc_ref = fs.collection("foo").document("first")
    async with fs.batch() as batch:
        batch.set(doc_ref, {"id": 1})
    doc = await doc_ref.get()
    assert doc.to_dict() == {"id": 1}


@pytest.mark.asyncio
async def test_write_batch_update_and_delete(fs):
    await fs.collection("foo").document("first").set({"id": 1})
    await fs.collection("foo").document("second").set({"id": 2})

    first = fs.collection("foo").document("first")
    second = fs.collection("foo").document("second")
    async with fs.batch() as batch:
        batch.update(first, {"updated": True})
        batch.delete(second)

    doc1 = await first.get()
    doc2 = await second.get()
    assert doc1.to_dict() == {"id": 1, "updated": True}
    assert doc2.exists is False
