from fake_firestore import MockFirestore


def test_contract_collection_group_returns_docs_from_multiple_parents(
    fs: MockFirestore, collection_name: str
) -> None:
    parent_one = fs.collection("parents").document("one")
    parent_two = fs.collection("other_parents").document("two")

    parent_one.collection(collection_name).document("a").set({"id": 1})
    parent_two.collection(collection_name).document("b").set({"id": 2})

    snapshots = list(fs.collection_group(collection_name).stream())
    results = {snapshot.to_dict()["id"] for snapshot in snapshots}

    assert results == {1, 2}


def test_contract_order_by_with_where_filters_and_sorts(
    fs: MockFirestore, collection_name: str
) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 2, "active": True})
    base.document("b").set({"score": 1, "active": True})
    base.document("c").set({"score": 3, "active": False})

    snapshots = list(base.where("active", "==", True).order_by("score").stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [1, 2]


def test_contract_order_by_descending(fs: MockFirestore, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 2})
    base.document("b").set({"score": 1})
    base.document("c").set({"score": 3})

    snapshots = list(base.order_by("score", direction="DESCENDING").stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [3, 2, 1]


def test_contract_limit(fs: MockFirestore, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 2})
    base.document("b").set({"score": 1})
    base.document("c").set({"score": 3})

    snapshots = list(base.order_by("score").limit(2).stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [1, 2]


def test_contract_start_at_end_at(fs: MockFirestore, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 1})
    base.document("b").set({"score": 2})
    base.document("c").set({"score": 3})

    start_snapshot = base.document("b").get()
    end_snapshot = base.document("c").get()

    snapshots = list(base.order_by("score").start_at(start_snapshot).end_at(end_snapshot).stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [2, 3]


def test_contract_array_contains(fs: MockFirestore, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"tags": ["red", "blue"]})
    base.document("b").set({"tags": ["green"]})
    base.document("c").set({"tags": ["blue"]})

    snapshots = list(base.where("tags", "array_contains", "blue").stream())
    ids = {snapshot.id for snapshot in snapshots}

    assert ids == {"a", "c"}


def test_contract_query_get_returns_list(fs: MockFirestore, collection_name: str) -> None:
    """query.get() should return a list, not a generator."""
    base = fs.collection(collection_name)
    base.document("a").set({"active": True})
    base.document("b").set({"active": False})

    result = base.where("active", "==", True).get()

    assert isinstance(result, list)
    assert len(result) == 1
