from unittest import TestCase

from fake_firestore import FakeFirestoreClient, MockFirestore


class TestMockFirestore(TestCase):
    def test_client_get_all(self):
        fs = MockFirestore()
        fs._data = {"foo": {"first": {"id": 1}, "second": {"id": 2}}}
        doc = fs.collection("foo").document("first")
        results = list(fs.get_all([doc]))
        returned_doc_snapshot = results[0].to_dict()
        expected_doc_snapshot = doc.get().to_dict()
        self.assertEqual(returned_doc_snapshot, expected_doc_snapshot)

    def test_client_collections(self):
        fs = MockFirestore()
        fs._data = {"foo": {"first": {"id": 1}, "second": {"id": 2}}, "bar": {}}
        collections = fs.collections()
        expected_collections = fs._data

        self.assertEqual(len(collections), len(expected_collections))
        for collection in collections:
            self.assertTrue(collection._path[0] in expected_collections)

    def test_collection_group_basic(self):
        fs = FakeFirestoreClient()
        # Create documents in nested collections with the same name
        fs.collection("parents").document("parent1").set({"name": "Parent 1"})
        fs.collection("parents").document("parent1").collection("children").document("child1").set(
            {"name": "Child 1", "age": 10}
        )
        fs.collection("parents").document("parent2").set({"name": "Parent 2"})
        fs.collection("parents").document("parent2").collection("children").document("child2").set(
            {"name": "Child 2", "age": 15}
        )

        # Query across all "children" collections
        children = list(fs.collection_group("children").stream())
        self.assertEqual(len(children), 2)
        names = {child.to_dict()["name"] for child in children}
        self.assertEqual(names, {"Child 1", "Child 2"})

    def test_collection_group_with_where(self):
        fs = FakeFirestoreClient()
        fs.collection("parents").document("parent1").collection("children").document("child1").set(
            {"name": "Tom", "age": 17}
        )
        fs.collection("parents").document("parent2").collection("children").document("child2").set(
            {"name": "Jerry", "age": 17}
        )
        fs.collection("parents").document("parent3").collection("children").document("child3").set(
            {"name": "Spike", "age": 25}
        )

        # Query children with age == 17
        children = list(fs.collection_group("children").where("age", "==", 17).stream())
        self.assertEqual(len(children), 2)
        names = {child.to_dict()["name"] for child in children}
        self.assertEqual(names, {"Tom", "Jerry"})

    def test_collection_group_empty(self):
        fs = FakeFirestoreClient()
        fs.collection("parents").document("parent1").set({"name": "Parent 1"})

        # Query non-existent collection group
        children = list(fs.collection_group("children").stream())
        self.assertEqual(len(children), 0)

    def test_collection_group_with_order_and_limit(self):
        fs = FakeFirestoreClient()
        fs.collection("users").document("u1").collection("posts").document("p1").set(
            {"title": "Post A", "likes": 10}
        )
        fs.collection("users").document("u2").collection("posts").document("p2").set(
            {"title": "Post B", "likes": 30}
        )
        fs.collection("users").document("u3").collection("posts").document("p3").set(
            {"title": "Post C", "likes": 20}
        )

        # Get top 2 posts by likes
        posts = list(
            fs.collection_group("posts").order_by("likes", direction="DESCENDING").limit(2).stream()
        )
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0].to_dict()["title"], "Post B")
        self.assertEqual(posts[1].to_dict()["title"], "Post C")

    def test_collection_group_invalid_id_with_slash(self):
        fs = FakeFirestoreClient()
        with self.assertRaises(ValueError) as context:
            fs.collection_group("invalid/id")
        self.assertIn("must not contain '/'", str(context.exception))
