import pytest

from fake_firestore import MockFirestore, NotFound


def test_contract_batch_set_update_delete_commit(fs: MockFirestore, collection_name: str) -> None:
    first = fs.collection(collection_name).document("first")
    second = fs.collection(collection_name).document("second")

    first.set({"id": 1})
    second.set({"id": 2})

    batch = fs.batch()
    batch.set(first, {"id": 1, "name": "one"})
    batch.update(second, {"active": True})
    batch.delete(first)

    results = batch.commit()

    assert len(results) == 3
    assert first.get().exists is False
    assert second.get().to_dict() == {"id": 2, "active": True}


def test_contract_batch_commit_no_ops(fs: MockFirestore, collection_name: str) -> None:
    batch = fs.batch()

    results = batch.commit()

    assert results == []


def test_contract_batch_context_manager_commits(fs: MockFirestore, collection_name: str) -> None:
    doc_ref = fs.collection(collection_name).document("ctx")

    with fs.batch() as batch:
        batch.set(doc_ref, {"id": 10})

    assert doc_ref.get().to_dict() == {"id": 10}


def test_contract_batch_update_missing_raises_not_found(
    fs: MockFirestore, collection_name: str
) -> None:
    doc_ref = fs.collection(collection_name).document("missing")
    batch = fs.batch()
    batch.update(doc_ref, {"active": True})

    with pytest.raises(NotFound):
        batch.commit()
