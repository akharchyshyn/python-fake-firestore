# by analogy with
# https://github.com/mongomock/mongomock/blob/develop/mongomock/__init__.py
# try to import gcloud exceptions
# and if gcloud is not installed, define our own
try:
    from google.api_core.exceptions import (
        AlreadyExists,
        ClientError,
        Conflict,
        NotFound,
    )
except ImportError:  # pragma: no cover
    from fake_firestore.exceptions import (  # type: ignore[assignment]
        AlreadyExists,
        ClientError,
        Conflict,
        NotFound,
    )

from fake_firestore._helpers import Timestamp
from fake_firestore.client import MockFirestore
from fake_firestore.collection import CollectionReference
from fake_firestore.document import DocumentReference, DocumentSnapshot
from fake_firestore.query import Query
from fake_firestore.transaction import Transaction

__all__ = [
    "AlreadyExists",
    "ClientError",
    "CollectionReference",
    "Conflict",
    "DocumentReference",
    "DocumentSnapshot",
    "MockFirestore",
    "NotFound",
    "Query",
    "Timestamp",
    "Transaction",
]
