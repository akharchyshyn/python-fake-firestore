from __future__ import annotations

import os
import uuid
from typing import Union

import pytest
from google.cloud.firestore import Client as FirestoreClient

from fake_firestore import FakeFirestoreClient

FirestoreDB = Union[FakeFirestoreClient, FirestoreClient]


@pytest.fixture
def fs() -> FirestoreDB:
    backend = os.getenv("FIRESTORE_BACKEND", "fake")
    if backend == "fake":
        return FakeFirestoreClient()
    if backend == "emulator":
        emulator_host = os.getenv("FIRESTORE_EMULATOR_HOST")
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        if not emulator_host or not project_id:
            pytest.skip(
                "Firestore emulator env not set. "
                "Set FIRESTORE_EMULATOR_HOST and GOOGLE_CLOUD_PROJECT."
            )
        from google.cloud import firestore

        return firestore.Client(project=project_id)
    raise ValueError(f"Unsupported FIRESTORE_BACKEND: {backend}")


@pytest.fixture
def collection_name() -> str:
    return f"contract_{uuid.uuid4().hex}"
