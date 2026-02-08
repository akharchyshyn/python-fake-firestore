"""Contract tests for async Firestore client.

These tests run against both the fake (AsyncFakeFirestoreClient) and the real
emulator (google.cloud.firestore_v1.AsyncClient) to verify behavioral parity.
"""

from __future__ import annotations

import pytest

from tests.contract.conftest import AsyncFirestoreDB


@pytest.mark.asyncio
async def test_async_contract_set_then_get(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    doc_ref = async_fs.collection(collection_name).document("alice")
    payload = {"name": "Alice", "age": 30}
    await doc_ref.set(payload)
    snapshot = await doc_ref.get()
    assert snapshot.to_dict() == payload


@pytest.mark.asyncio
async def test_async_contract_get_missing_doc(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    doc_ref = async_fs.collection(collection_name).document("missing")
    snapshot = await doc_ref.get()
    assert snapshot.exists is False


@pytest.mark.asyncio
async def test_async_contract_update_missing_raises(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    from fake_firestore import NotFound

    doc_ref = async_fs.collection(collection_name).document("missing")
    with pytest.raises((NotFound, Exception)):
        await doc_ref.update({"active": True})


@pytest.mark.asyncio
async def test_async_contract_create_new_doc(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    doc_ref = async_fs.collection(collection_name).document("new_doc")
    payload = {"name": "Bob"}
    await doc_ref.create(payload)
    snapshot = await doc_ref.get()
    assert snapshot.exists is True
    assert snapshot.to_dict() == payload


@pytest.mark.asyncio
async def test_async_contract_create_existing_raises(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    from fake_firestore import AlreadyExists

    doc_ref = async_fs.collection(collection_name).document("existing")
    await doc_ref.set({"name": "Alice"})
    with pytest.raises((AlreadyExists, Exception)):
        await doc_ref.create({"name": "Bob"})


@pytest.mark.asyncio
async def test_async_contract_delete(async_fs: AsyncFirestoreDB, collection_name: str) -> None:
    doc_ref = async_fs.collection(collection_name).document("to_delete")
    await doc_ref.set({"x": 1})
    await doc_ref.delete()
    snapshot = await doc_ref.get()
    assert snapshot.exists is False


@pytest.mark.asyncio
async def test_async_contract_stream(async_fs: AsyncFirestoreDB, collection_name: str) -> None:
    await async_fs.collection(collection_name).document("a").set({"id": 1})
    await async_fs.collection(collection_name).document("b").set({"id": 2})
    docs = [doc async for doc in async_fs.collection(collection_name).stream()]
    assert len(docs) == 2


@pytest.mark.asyncio
async def test_async_contract_set_merge(async_fs: AsyncFirestoreDB, collection_name: str) -> None:
    doc_ref = async_fs.collection(collection_name).document("merge_test")
    await doc_ref.set({"name": "Alice", "age": 30})
    await doc_ref.set({"age": 31}, merge=True)
    snapshot = await doc_ref.get()
    assert snapshot.to_dict() == {"name": "Alice", "age": 31}


@pytest.mark.asyncio
async def test_async_contract_batch_commit(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    batch = async_fs.batch()
    doc1 = async_fs.collection(collection_name).document("b1")
    doc2 = async_fs.collection(collection_name).document("b2")
    batch.set(doc1, {"id": 1})
    batch.set(doc2, {"id": 2})
    await batch.commit()
    snap1 = await doc1.get()
    snap2 = await doc2.get()
    assert snap1.to_dict() == {"id": 1}
    assert snap2.to_dict() == {"id": 2}


@pytest.mark.asyncio
async def test_async_contract_transaction(async_fs: AsyncFirestoreDB, collection_name: str) -> None:
    doc_ref = async_fs.collection(collection_name).document("txn_doc")
    await doc_ref.set({"count": 0})
    async with async_fs.transaction() as txn:
        txn._begin()
        txn.set(doc_ref, {"count": 1})
    snap = await doc_ref.get()
    assert snap.to_dict() == {"count": 1}


@pytest.mark.asyncio
async def test_async_contract_add(async_fs: AsyncFirestoreDB, collection_name: str) -> None:
    timestamp, doc_ref = await async_fs.collection(collection_name).add(
        {"name": "test"}, document_id="explicit_id"
    )
    snapshot = await doc_ref.get()
    assert snapshot.exists is True
    assert snapshot.to_dict() == {"name": "test"}


@pytest.mark.asyncio
async def test_async_contract_query_where(async_fs: AsyncFirestoreDB, collection_name: str) -> None:
    await async_fs.collection(collection_name).document("a").set({"score": 10})
    await async_fs.collection(collection_name).document("b").set({"score": 20})
    await async_fs.collection(collection_name).document("c").set({"score": 30})
    query = async_fs.collection(collection_name).where("score", ">=", 20)
    docs = [doc async for doc in query.stream()]
    assert len(docs) == 2
    scores = {doc.to_dict()["score"] for doc in docs}
    assert scores == {20, 30}


@pytest.mark.asyncio
async def test_async_contract_order_by_limit(
    async_fs: AsyncFirestoreDB, collection_name: str
) -> None:
    await async_fs.collection(collection_name).document("a").set({"order": 3})
    await async_fs.collection(collection_name).document("b").set({"order": 1})
    await async_fs.collection(collection_name).document("c").set({"order": 2})
    query = async_fs.collection(collection_name).order_by("order").limit(2)
    docs = [doc async for doc in query.stream()]
    assert len(docs) == 2
    assert docs[0].to_dict()["order"] == 1
    assert docs[1].to_dict()["order"] == 2
