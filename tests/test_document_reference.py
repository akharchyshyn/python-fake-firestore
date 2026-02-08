from unittest import TestCase

from google.cloud import firestore

from fake_firestore import AlreadyExists, MockFirestore, NotFound


class TestDocumentReference(TestCase):
    def test_get_document_by_path(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1}, timeout=5.0)
        doc = fs.document("foo/first").get(timeout=5.0)
        self.assertEqual({"id": 1}, doc.to_dict())
        self.assertEqual("first", doc.id)

    def test_set_document_by_path(self):
        fs = MockFirestore()
        doc_content = {"id": "bar"}
        fs.document("foo/doc1/bar/doc2").set(doc_content, timeout=5.0)
        doc = fs.document("foo/doc1/bar/doc2").get(timeout=5.0).to_dict()
        self.assertEqual(doc_content, doc)

    def test_document_get_returnsDocument(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        doc = fs.collection("foo").document("first").get()
        self.assertEqual({"id": 1}, doc.to_dict())
        self.assertEqual("first", doc.id)

    def test_document_get_documentIdEqualsKey(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        doc_ref = fs.collection("foo").document("first")
        self.assertEqual("first", doc_ref.id)

    def test_document_get_newDocumentReturnsDefaultId(self):
        fs = MockFirestore()
        doc_ref = fs.collection("foo").document()
        doc = doc_ref.get()
        self.assertNotEqual(None, doc_ref.id)
        self.assertFalse(doc.exists)

    def test_document_get_documentDoesNotExist(self):
        fs = MockFirestore()
        doc = fs.collection("foo").document("bar").get().to_dict()
        self.assertIsNone(doc)

    def test_get_nestedDocument(self):
        fs = MockFirestore()
        fs.collection("top_collection").document("top_document").set({"id": 1})
        fs.collection("top_collection").document("top_document").collection(
            "nested_collection"
        ).document("nested_document").set({"id": 1.1})
        doc = (
            fs.collection("top_collection")
            .document("top_document")
            .collection("nested_collection")
            .document("nested_document")
            .get()
            .to_dict()
        )

        self.assertEqual({"id": 1.1}, doc)

    def test_get_nestedDocument_documentDoesNotExist(self):
        fs = MockFirestore()
        fs.collection("top_collection").document("top_document").set({"id": 1})
        doc = (
            fs.collection("top_collection")
            .document("top_document")
            .collection("nested_collection")
            .document("nested_document")
            .get()
            .to_dict()
        )

        self.assertIsNone(doc)

    def test_document_get_with_field_paths(self):
        fs = MockFirestore()
        fs.collection("foo").document("doc").set({"name": "Alice", "age": 30, "city": "NYC"})
        snapshot = fs.collection("foo").document("doc").get(field_paths=["name", "city"])
        self.assertTrue(snapshot.exists)
        self.assertEqual({"name": "Alice", "city": "NYC"}, snapshot.to_dict())

    def test_document_get_with_field_paths_missing_doc(self):
        fs = MockFirestore()
        snapshot = fs.collection("foo").document("missing").get(field_paths=["name"])
        self.assertFalse(snapshot.exists)
        self.assertIsNone(snapshot.to_dict())

    def test_document_get_with_empty_field_paths(self):
        fs = MockFirestore()
        fs.collection("foo").document("doc").set({"name": "Alice", "age": 30})
        snapshot = fs.collection("foo").document("doc").get(field_paths=[])
        self.assertTrue(snapshot.exists)
        self.assertEqual({}, snapshot.to_dict())

    def test_document_get_accepts_timeout(self):
        fs = MockFirestore()
        fs.collection("foo").document("doc").set({"x": 1})
        snapshot = fs.collection("foo").document("doc").get(timeout=10.0)
        self.assertTrue(snapshot.exists)
        self.assertEqual({"x": 1}, snapshot.to_dict())

    def test_document_get_accepts_retry(self):
        fs = MockFirestore()
        fs.collection("foo").document("doc").set({"x": 1})
        snapshot = fs.collection("foo").document("doc").get(retry=None)
        self.assertTrue(snapshot.exists)
        self.assertEqual({"x": 1}, snapshot.to_dict())

    def test_document_set_setsContentOfDocument(self):
        fs = MockFirestore()
        doc_content = {"id": "bar"}
        fs.collection("foo").document("bar").set(doc_content)
        doc = fs.collection("foo").document("bar").get().to_dict()
        self.assertEqual(doc_content, doc)

    def test_document_set_mergeNewValue(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        fs.collection("foo").document("first").set({"updated": True}, merge=True)
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"id": 1, "updated": True}, doc)

    def test_document_set_mergeNewValueForNonExistentDoc(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"updated": True}, merge=True)
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"updated": True}, doc)

    def test_document_set_overwriteValue(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        fs.collection("foo").document("first").set({"new_id": 1}, merge=False)
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"new_id": 1}, doc)

    def test_document_set_isolation(self):
        fs = MockFirestore()
        doc_content = {"id": "bar"}
        fs.collection("foo").document("bar").set(doc_content)
        doc_content["id"] = "new value"
        doc = fs.collection("foo").document("bar").get().to_dict()
        self.assertEqual({"id": "bar"}, doc)

    def test_document_update_addNewValue(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        fs.collection("foo").document("first").update({"updated": True}, timeout=5.0)
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"id": 1, "updated": True}, doc)

    def test_document_update_changeExistingValue(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        fs.collection("foo").document("first").update({"id": 2})
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"id": 2}, doc)

    def test_document_update_documentDoesNotExist(self):
        fs = MockFirestore()
        with self.assertRaises(NotFound):
            fs.collection("foo").document("nonexistent").update({"id": 2})
        docsnap = fs.collection("foo").document("nonexistent").get()
        self.assertFalse(docsnap.exists)

    def test_document_update_isolation(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"nested": {"id": 1}})
        update_doc = {"nested": {"id": 2}}
        fs.collection("foo").document("first").update(update_doc)
        update_doc["nested"]["id"] = 3
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"nested": {"id": 2}}, doc)

    def test_document_update_transformerIncrementBasic(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"count": 1})
        fs.collection("foo").document("first").update({"count": firestore.Increment(2)})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"count": 3})

    def test_document_update_transformerIncrementNested(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set(
            {
                "nested": {"count": 1},
                "other": {"likes": 0},
            }
        )
        fs.collection("foo").document("first").update(
            {
                "nested": {"count": firestore.Increment(-1)},
                "other": {"likes": firestore.Increment(1), "smoked": "salmon"},
            }
        )

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"nested": {"count": 0}, "other": {"likes": 1, "smoked": "salmon"}})

    def test_document_update_transformerIncrementNonExistent(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"spicy": "tuna"})
        fs.collection("foo").document("first").update({"count": firestore.Increment(1)})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"count": 1, "spicy": "tuna"})

    def test_document_set_transformerIncrementOnNewDoc(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set(
            {"count": firestore.Increment(1), "nested": {"score": firestore.Increment(5)}}
        )

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc["count"], 1)
        self.assertEqual(doc["nested"]["score"], 5)

    def test_document_collections(self):
        fs = MockFirestore()
        doc_ref = fs.collection("foo").document("parent")
        doc_ref.collection("sub_a").document("child1").set({"x": 1})
        doc_ref.collection("sub_b").document("child2").set({"x": 2})

        subcollections = list(doc_ref.collections())
        ids = {col.id for col in subcollections}
        self.assertEqual(ids, {"sub_a", "sub_b"})

    def test_document_collections_empty(self):
        fs = MockFirestore()
        doc_ref = fs.collection("foo").document("leaf")
        doc_ref.set({"name": "leaf"})

        subcollections = list(doc_ref.collections())
        self.assertEqual(subcollections, [])

    def test_document_delete_documentDoesNotExistAfterDelete(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        fs.collection("foo").document("first").delete(timeout=5.0)
        doc = fs.collection("foo").document("first").get()
        self.assertEqual(False, doc.exists)

    def test_document_parent(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        coll = fs.collection("foo")
        document = coll.document("first")
        self.assertIs(document.parent, coll)

    def test_document_create_createsNewDocument(self):
        fs = MockFirestore()
        doc_content = {"id": "bar"}
        fs.collection("foo").document("bar").create(doc_content, timeout=5.0)
        doc = fs.collection("foo").document("bar").get().to_dict()
        self.assertEqual(doc_content, doc)

    def test_document_create_raisesAlreadyExistsIfDocumentExists(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        with self.assertRaises(AlreadyExists):
            fs.collection("foo").document("first").create({"id": 2})
        # Verify the original document is unchanged
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual({"id": 1}, doc)

    def test_document_create_isolation(self):
        fs = MockFirestore()
        doc_content = {"id": "bar"}
        fs.collection("foo").document("bar").create(doc_content)
        doc_content["id"] = "new value"
        doc = fs.collection("foo").document("bar").get().to_dict()
        self.assertEqual({"id": "bar"}, doc)

    def test_document_update_transformerArrayUnionBasic(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"arr": [1, 2]})
        fs.collection("foo").document("first").update({"arr": firestore.ArrayUnion([3, 4])})
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc["arr"], [1, 2, 3, 4])

    def test_document_update_transformerArrayUnionDuplicates(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"arr": [1, 3]})
        fs.collection("foo").document("first").update({"arr": firestore.ArrayUnion([2, 3, 4])})
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc["arr"], [1, 3, 2, 4])

    def test_document_update_transformerArrayUnionNested(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set(
            {
                "nested": {"arr": [1]},
                "other": {"labels": ["a"]},
            }
        )
        fs.collection("foo").document("first").update(
            {
                "nested": {"arr": firestore.ArrayUnion([2])},
                "other": {"labels": firestore.ArrayUnion(["b"]), "smoked": "salmon"},
            }
        )

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(
            doc, {"nested": {"arr": [1, 2]}, "other": {"labels": ["a", "b"], "smoked": "salmon"}}
        )

    def test_document_update_transformerArrayUnionNonExistent(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"spicy": "tuna"})
        fs.collection("foo").document("first").update({"arr": firestore.ArrayUnion([1])})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"arr": [1], "spicy": "tuna"})

    def test_document_update_nestedFieldDotNotation(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"nested": {"value": 1, "unchanged": "foo"}})

        fs.collection("foo").document("first").update({"nested.value": 2})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"nested": {"value": 2, "unchanged": "foo"}})

    def test_document_update_nestedFieldDotNotationNestedFieldCreation(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"other": None})

        fs.collection("foo").document("first").update({"nested.value": 2})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"nested": {"value": 2}, "other": None})

    def test_document_update_nestedFieldDotNotationMultipleNested(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"other": None})

        fs.collection("foo").document("first").update({"nested.subnested.value": 42})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"nested": {"subnested": {"value": 42}}, "other": None})

    def test_document_update_nestedFieldDotNotationMultipleNestedWithTransformer(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"other": None})

        fs.collection("foo").document("first").update(
            {"nested.subnested.value": firestore.ArrayUnion([1, 3])}
        )

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {"nested": {"subnested": {"value": [1, 3]}}, "other": None})

    def test_document_update_transformerSentinel(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"spicy": "tuna"})
        fs.collection("foo").document("first").update({"spicy": firestore.DELETE_FIELD})

        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc, {})

    def test_document_update_transformerArrayRemoveBasic(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"arr": [1, 2, 3, 4]})
        fs.collection("foo").document("first").update({"arr": firestore.ArrayRemove([3, 4])})
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc["arr"], [1, 2])

    def test_document_update_transformerArrayRemoveNonExistentField(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"arr": [1, 2, 3, 4]})
        fs.collection("foo").document("first").update({"arr": firestore.ArrayRemove([5])})
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc["arr"], [1, 2, 3, 4])

    def test_document_update_transformerArrayRemoveNonExistentArray(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"arr": [1, 2, 3, 4]})
        fs.collection("foo").document("first").update(
            {"non_existent_array": firestore.ArrayRemove([1, 2])}
        )
        doc = fs.collection("foo").document("first").get().to_dict()
        self.assertEqual(doc["arr"], [1, 2, 3, 4])

    def test_document_path_property(self):
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        doc_ref = fs.collection("foo").document("first")
        self.assertEqual("foo/first", doc_ref.path)

    def test_document_path_nested(self):
        fs = MockFirestore()
        doc_ref = fs.collection("foo").document("first").collection("bar").document("second")
        self.assertEqual("foo/first/bar/second", doc_ref.path)

    def test_document_reference_equality(self):
        fs = MockFirestore()
        ref1 = fs.collection("foo").document("bar")
        ref2 = fs.collection("foo").document("bar")
        self.assertEqual(ref1, ref2)
        self.assertEqual(hash(ref1), hash(ref2))

    def test_document_reference_inequality(self):
        fs = MockFirestore()
        ref1 = fs.collection("foo").document("a")
        ref2 = fs.collection("foo").document("b")
        self.assertNotEqual(ref1, ref2)

    def test_document_reference_not_equal_to_other_type(self):
        fs = MockFirestore()
        ref = fs.collection("foo").document("bar")
        self.assertNotEqual(ref, "foo/bar")

    def test_where_by_document_reference(self):
        fs = MockFirestore()
        target_ref = fs.collection("foo").document("target")
        target_ref.set({"name": "target"})
        fs.collection("foo").document("a").set({"ref": target_ref})
        fs.collection("foo").document("b").set({"ref": "not_a_ref"})

        query_ref = fs.collection("foo").document("target")
        docs = list(fs.collection("foo").where("ref", "==", query_ref).stream())
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].id, "a")
