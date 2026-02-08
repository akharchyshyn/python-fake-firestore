from google.cloud.firestore_v1.base_query import FieldFilter

from tests.contract.conftest import FirestoreDB


def test_contract_collection_group_returns_docs_from_multiple_parents(
    fs: FirestoreDB, collection_name: str
) -> None:
    parent_one = fs.collection("parents").document("one")
    parent_two = fs.collection("other_parents").document("two")

    parent_one.collection(collection_name).document("a").set({"id": 1})
    parent_two.collection(collection_name).document("b").set({"id": 2})

    snapshots = list(fs.collection_group(collection_name).stream())
    results = {snapshot.to_dict()["id"] for snapshot in snapshots}

    assert results == {1, 2}


def test_contract_order_by_with_where_filters_and_sorts(
    fs: FirestoreDB, collection_name: str
) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 2, "active": True})
    base.document("b").set({"score": 1, "active": True})
    base.document("c").set({"score": 3, "active": False})

    snapshots = list(base.where("active", "==", True).order_by("score").stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [1, 2]


def test_contract_order_by_descending(fs: FirestoreDB, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 2})
    base.document("b").set({"score": 1})
    base.document("c").set({"score": 3})

    snapshots = list(base.order_by("score", direction="DESCENDING").stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [3, 2, 1]


def test_contract_limit(fs: FirestoreDB, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 2})
    base.document("b").set({"score": 1})
    base.document("c").set({"score": 3})

    snapshots = list(base.order_by("score").limit(2).stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [1, 2]


def test_contract_start_at_end_at(fs: FirestoreDB, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"score": 1})
    base.document("b").set({"score": 2})
    base.document("c").set({"score": 3})

    start_snapshot = base.document("b").get()
    end_snapshot = base.document("c").get()

    snapshots = list(base.order_by("score").start_at(start_snapshot).end_at(end_snapshot).stream())
    scores = [snapshot.to_dict()["score"] for snapshot in snapshots]

    assert scores == [2, 3]


def test_contract_array_contains(fs: FirestoreDB, collection_name: str) -> None:
    base = fs.collection(collection_name)
    base.document("a").set({"tags": ["red", "blue"]})
    base.document("b").set({"tags": ["green"]})
    base.document("c").set({"tags": ["blue"]})

    snapshots = list(base.where("tags", "array_contains", "blue").stream())
    ids = {snapshot.id for snapshot in snapshots}

    assert ids == {"a", "c"}


def test_contract_query_get_returns_list(fs: FirestoreDB, collection_name: str) -> None:
    """query.get() should return a list, not a generator."""
    base = fs.collection(collection_name)
    base.document("a").set({"active": True})
    base.document("b").set({"active": False})

    result = base.where("active", "==", True).get()

    assert isinstance(result, list)
    assert len(result) == 1


def test_contract_array_contains_skips_none_fields(fs: FirestoreDB, collection_name: str) -> None:
    """array_contains should skip documents where the field is None."""
    base = fs.collection(collection_name)
    base.document("a").set({"tags": ["red", "blue"]})
    base.document("b").set({"tags": None})
    base.document("c").set({"tags": ["blue"]})
    base.document("d").set({"other": "no tags field"})

    snapshots = list(base.where("tags", "array_contains", "blue").stream())
    ids = {snapshot.id for snapshot in snapshots}

    assert ids == {"a", "c"}


def test_contract_array_contains_any_skips_none_fields(
    fs: FirestoreDB, collection_name: str
) -> None:
    """array_contains_any should skip documents where the field is None."""
    base = fs.collection(collection_name)
    base.document("a").set({"tags": ["red", "blue"]})
    base.document("b").set({"tags": None})
    base.document("c").set({"tags": ["green"]})

    snapshots = list(base.where("tags", "array_contains_any", ["red", "green"]).stream())
    ids = {snapshot.id for snapshot in snapshots}

    assert ids == {"a", "c"}


def test_contract_select_returns_only_specified_fields(
    fs: FirestoreDB, collection_name: str
) -> None:
    """select() should return snapshots containing only the requested fields."""
    base = fs.collection(collection_name)
    base.document("a").set({"name": "Alice", "age": 30, "city": "Kyiv"})
    base.document("b").set({"name": "Bob", "age": 25, "city": "Lviv"})

    snapshots = list(base.select(["name", "age"]).stream())

    assert len(snapshots) == 2
    for snap in snapshots:
        data = snap.to_dict()
        assert "name" in data
        assert "age" in data
        assert "city" not in data


def test_contract_select_empty_fields_returns_empty_dicts(
    fs: FirestoreDB, collection_name: str
) -> None:
    """select([]) should return snapshots with no fields (only verifies existence)."""
    base = fs.collection(collection_name)
    base.document("a").set({"name": "Alice"})

    snapshots = list(base.select([]).stream())

    assert len(snapshots) == 1
    assert snapshots[0].to_dict() == {}


def test_contract_select_with_where(fs: FirestoreDB, collection_name: str) -> None:
    """select() combined with where() should filter and project."""
    base = fs.collection(collection_name)
    base.document("a").set({"name": "Alice", "active": True, "score": 10})
    base.document("b").set({"name": "Bob", "active": False, "score": 20})
    base.document("c").set({"name": "Carol", "active": True, "score": 30})

    snapshots = list(base.where("active", "==", True).select(["name"]).stream())

    assert len(snapshots) == 2
    for snap in snapshots:
        data = snap.to_dict()
        assert "name" in data
        assert "active" not in data
        assert "score" not in data


def test_contract_where_with_field_filter(fs: FirestoreDB, collection_name: str) -> None:
    """where() should accept a FieldFilter via the filter keyword argument."""
    base = fs.collection(collection_name)
    base.document("a").set({"name": "Alice", "active": True})
    base.document("b").set({"name": "Bob", "active": False})
    base.document("c").set({"name": "Carol", "active": True})

    snapshots = list(base.where(filter=FieldFilter("active", "==", True)).stream())
    names = {snap.to_dict()["name"] for snap in snapshots}

    assert names == {"Alice", "Carol"}


def test_contract_where_chained_field_filters(fs: FirestoreDB, collection_name: str) -> None:
    """Chaining multiple where() calls with FieldFilter should apply all filters."""
    base = fs.collection(collection_name)
    base.document("a").set({"active": True, "score": 10})
    base.document("b").set({"active": True, "score": 20})
    base.document("c").set({"active": False, "score": 30})

    snapshots = list(
        base.where(filter=FieldFilter("active", "==", True))
        .where(filter=FieldFilter("score", ">=", 15))
        .stream()
    )

    assert len(snapshots) == 1
    assert snapshots[0].to_dict()["score"] == 20
