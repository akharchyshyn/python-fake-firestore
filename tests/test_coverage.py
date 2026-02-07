"""Tests for uncovered code paths."""

import warnings
from unittest import TestCase

from fake_firestore import MockFirestore, Transaction
from fake_firestore.transaction import WriteResult


class TestClientEdgeCases(TestCase):
    """Test edge cases in MockFirestore client."""

    def test_document_invalid_path_odd_segments(self):
        """Test that document() raises for odd number of path segments."""
        fs = MockFirestore()
        with self.assertRaises(Exception) as ctx:
            fs.document("foo")
        self.assertIn("Cannot create document", str(ctx.exception))

    def test_document_invalid_path_single_segment(self):
        """Test that document() raises for single segment path."""
        fs = MockFirestore()
        with self.assertRaises(Exception) as ctx:
            fs.document("foo/bar/baz")
        self.assertIn("Cannot create document", str(ctx.exception))

    def test_collection_invalid_path_even_segments(self):
        """Test that collection() raises for even number of path segments."""
        fs = MockFirestore()
        with self.assertRaises(Exception) as ctx:
            fs.collection("foo/bar")
        self.assertIn("Cannot create collection", str(ctx.exception))

    def test_reset(self):
        """Test reset() clears all data."""
        fs = MockFirestore()
        fs.collection("foo").document("bar").set({"id": 1})
        self.assertEqual(len(list(fs.collection("foo").stream())), 1)
        fs.reset()
        self.assertEqual(len(list(fs.collection("foo").stream())), 0)

    def test_transaction_with_kwargs(self):
        """Test transaction() with custom kwargs."""
        fs = MockFirestore()
        tx = fs.transaction(max_attempts=10, read_only=True)
        self.assertEqual(tx._max_attempts, 10)
        self.assertTrue(tx._read_only)


class TestCollectionDeprecated(TestCase):
    """Test deprecated Collection methods."""

    def test_collection_get_deprecated(self):
        """Test that Collection.get() emits deprecation warning."""
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            list(fs.collection("foo").get())
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("deprecated", str(w[0].message))


class TestQueryEdgeCases(TestCase):
    """Test edge cases in Query."""

    def test_query_get_deprecated(self):
        """Test that Query.get() emits deprecation warning."""
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            list(fs.collection("foo").where("id", "==", 1).get())
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("deprecated", str(w[0].message))

    def test_query_unknown_operator(self):
        """Test that unknown operator raises ValueError."""
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})

        with self.assertRaises(ValueError) as ctx:
            list(fs.collection("foo").where("id", "unknown_op", 1).stream())
        self.assertIn("Unknown operator", str(ctx.exception))

    def test_query_cursor_no_match(self):
        """Test cursor with no matching document returns empty when no match."""
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        fs.collection("foo").document("second").set({"id": 2})
        # start_at with non-existent value - cursor not found, returns empty
        docs = list(fs.collection("foo").start_at({"id": 999}).stream())
        self.assertEqual(len(docs), 0)

    def test_query_chained_order_by(self):
        """Test chained order_by method."""
        fs = MockFirestore()
        fs.collection("foo").document("a").set({"id": 1, "name": "z"})
        fs.collection("foo").document("b").set({"id": 2, "name": "y"})
        fs.collection("foo").document("c").set({"id": 3, "name": "x"})
        query = fs.collection("foo").order_by("id").order_by("name")
        docs = list(query.stream())
        self.assertEqual(len(docs), 3)

    def test_query_chained_limit_offset(self):
        """Test chained limit and offset methods."""
        fs = MockFirestore()
        fs.collection("foo").document("a").set({"id": 1})
        fs.collection("foo").document("b").set({"id": 2})
        fs.collection("foo").document("c").set({"id": 3})
        fs.collection("foo").document("d").set({"id": 4})
        # Test limit
        query = fs.collection("foo").limit(2)
        docs = list(query.stream())
        self.assertEqual(len(docs), 2)

        # Test offset
        query = fs.collection("foo").offset(2)
        docs = list(query.stream())
        self.assertEqual(len(docs), 2)

    def test_query_chained_where(self):
        """Test chained where method on Query object."""
        fs = MockFirestore()
        fs.collection("foo").document("a").set({"id": 1, "active": True})
        fs.collection("foo").document("b").set({"id": 2, "active": True})
        fs.collection("foo").document("c").set({"id": 3, "active": False})
        # Chain multiple where clauses
        query = fs.collection("foo").where("active", "==", True).where("id", ">", 1)
        docs = list(query.stream())
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0].to_dict()["id"], 2)


class TestTransactionEdgeCases(TestCase):
    """Test edge cases in Transaction."""

    def test_transaction_id_property(self):
        """Test transaction id property."""
        fs = MockFirestore()
        tx = Transaction(fs)
        self.assertIsNone(tx.id)
        tx._begin()
        self.assertIsNotNone(tx.id)

    def test_transaction_rollback(self):
        """Test transaction _rollback()."""
        fs = MockFirestore()
        tx = Transaction(fs)
        tx._begin()
        self.assertTrue(tx.in_progress)
        tx._rollback()
        self.assertFalse(tx.in_progress)

    def test_transaction_rollback_not_in_progress(self):
        """Test _rollback() raises when not in progress."""
        fs = MockFirestore()
        tx = Transaction(fs)
        with self.assertRaises(ValueError) as ctx:
            tx._rollback()
        self.assertIn("rolled back", str(ctx.exception))

    def test_transaction_commit_not_in_progress(self):
        """Test _commit() raises when not in progress."""
        fs = MockFirestore()
        tx = Transaction(fs)
        with self.assertRaises(ValueError) as ctx:
            tx._commit()
        self.assertIn("committed", str(ctx.exception))

    def test_transaction_get_invalid_type(self):
        """Test transaction.get() with invalid type raises."""
        fs = MockFirestore()
        tx = Transaction(fs)
        tx._begin()
        with self.assertRaises(ValueError) as ctx:
            tx.get("invalid")  # type: ignore[arg-type]
        self.assertIn("must be a DocumentReference or a Query", str(ctx.exception))

    def test_transaction_read_only(self):
        """Test read-only transaction raises on write."""
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        tx = Transaction(fs, read_only=True)
        tx._begin()
        doc_ref = fs.collection("foo").document("first")
        with self.assertRaises(ValueError) as ctx:
            tx.set(doc_ref, {"id": 2})
        self.assertIn("read-only", str(ctx.exception))

    def test_transaction_create(self):
        """Test transaction.create() is no-op."""
        fs = MockFirestore()
        fs.collection("foo").document("first").set({"id": 1})
        tx = Transaction(fs)
        tx._begin()
        doc_ref = fs.collection("foo").document("first")
        # create is a no-op, should not raise
        tx.create(doc_ref, {"id": 2})

    def test_write_result(self):
        """Test WriteResult has update_time."""
        result = WriteResult()
        self.assertIsNotNone(result.update_time)


class TestExceptions(TestCase):
    """Test custom exceptions when google-cloud-firestore is not available."""

    def test_exceptions_import(self):
        """Test that exceptions module can be imported directly."""
        from fake_firestore.exceptions import (
            AlreadyExists,
            ClientError,
            Conflict,
            NotFound,
        )

        # Test exception hierarchy
        self.assertTrue(issubclass(Conflict, ClientError))
        self.assertTrue(issubclass(NotFound, ClientError))
        self.assertTrue(issubclass(AlreadyExists, Conflict))

    def test_exception_str(self):
        """Test exception string representation."""
        from fake_firestore.exceptions import NotFound

        exc = NotFound("Document not found")
        self.assertEqual(str(exc), "404 Document not found")

    def test_exception_codes(self):
        """Test exception codes."""
        from fake_firestore.exceptions import ClientError, Conflict, NotFound

        self.assertIsNone(ClientError.code)
        self.assertEqual(Conflict.code, 409)
        self.assertEqual(NotFound.code, 404)
