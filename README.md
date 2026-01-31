# Fake Firestore

[![CI](https://github.com/akharchyshyn/python-fake-firestore/actions/workflows/test.yml/badge.svg)](https://github.com/akharchyshyn/python-fake-firestore/actions/workflows/test.yml)
[![PyPI version](https://badge.fury.io/py/fake-firestore.svg)](https://pypi.org/project/fake-firestore/)
[![Python versions](https://img.shields.io/pypi/pyversions/fake-firestore.svg)](https://pypi.org/project/fake-firestore/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Fork of [mdowds/mock-firestore](https://github.com/mdowds/mock-firestore), originally `mock-firestore` on PyPI. This fork focuses on fake semantics and async facade.

This library provides an in-memory fake implementation of Firestore APIs for tests. It is not a mocking framework; it simulates Firestore behavior and stores documents in memory.

An in-memory implementation of the [Python client library](https://github.com/googleapis/python-firestore) for Google Cloud Firestore, intended for use in tests to replace the real thing. This project is in early stages and is only a partial implementation of the real client library.

## Installation

```bash
pip install fake-firestore
```

Python 3.8+ is required.

## Usage

```python
from fake_firestore import FakeFirestoreClient

db = FakeFirestoreClient()

# Can be used in the same way as a firestore.Client() object would be, e.g.:
db.collection('users').get()
```

To reset the store to an empty state, use the `reset()` method:
```python
db = FakeFirestoreClient()
db.reset()
```

> **Note:** `MockFirestore` is still available as a backward compatibility alias for `FakeFirestoreClient`.

## Supported operations

```python
from fake_firestore import FakeFirestoreClient

db = FakeFirestoreClient()

# Collections
db.collections()
db.collection('users')
db.collection('users').get()
db.collection('users').list_documents()
db.collection('users').stream()

# Documents
db.collection('users').document()
db.collection('users').document('alovelace')
db.collection('users').document('alovelace').id
db.collection('users').document('alovelace').parent
db.collection('users').document('alovelace').update_time
db.collection('users').document('alovelace').read_time
db.collection('users').document('alovelace').get()
db.collection('users').document('alovelace').get().exists
db.collection('users').document('alovelace').get().to_dict()
db.collection('users').document('alovelace').set({
    'first': 'Ada',
    'last': 'Lovelace'
})
db.collection('users').document('alovelace').set({'first': 'Augusta Ada'}, merge=True)
db.collection('users').document('alovelace').update({'born': 1815})
db.collection('users').document('alovelace').update({'favourite.color': 'red'})
db.collection('users').document('alovelace').update({'associates': ['Charles Babbage', 'Michael Faraday']})
db.collection('users').document('alovelace').collection('friends')
db.collection('users').document('alovelace').delete()
db.collection('users').document(document_id='alovelace').delete()
db.collection('users').add({'first': 'Ada', 'last': 'Lovelace'}, 'alovelace')
db.get_all([db.collection('users').document('alovelace')])
db.document('users/alovelace')
db.document('users/alovelace').update({'born': 1815})
db.collection('users/alovelace/friends')

# Querying
db.collection('users').order_by('born').get()
db.collection('users').order_by('born', direction='DESCENDING').get()
db.collection('users').limit(5).get()
db.collection('users').where('born', '==', 1815).get()
db.collection('users').where('born', '!=', 1815).get()
db.collection('users').where('born', '<', 1815).get()
db.collection('users').where('born', '>', 1815).get()
db.collection('users').where('born', '<=', 1815).get()
db.collection('users').where('born', '>=', 1815).get()
db.collection('users').where('born', 'in', [1815, 1900]).stream()
db.collection('users').where('associates', 'array_contains', 'Charles Babbage').stream()
db.collection('users').where('associates', 'array_contains_any', ['Charles Babbage', 'Michael Faraday']).stream()

# Transforms
from google.cloud import firestore
db.collection('users').document('alovelace').update({'likes': firestore.Increment(1)})
db.collection('users').document('alovelace').update({'associates': firestore.ArrayUnion(['Andrew Cross', 'Charles Wheatstone'])})
db.collection('users').document('alovelace').update({firestore.DELETE_FIELD: "born"})
db.collection('users').document('alovelace').update({'associates': firestore.ArrayRemove(['Andrew Cross'])})

# Cursors
db.collection('users').start_after({'id': 'alovelace'}).stream()
db.collection('users').end_before({'id': 'alovelace'}).stream()
db.collection('users').end_at({'id': 'alovelace'}).stream()
db.collection('users').start_after(db.collection('users').document('alovelace')).stream()

# Transactions
transaction = db.transaction()
transaction.id
transaction.in_progress
transaction.get(db.collection('users').where('born', '==', 1815))
transaction.get(db.collection('users').document('alovelace'))
transaction.get_all([db.collection('users').document('alovelace')])
transaction.set(db.collection('users').document('alovelace'), {'born': 1815})
transaction.update(db.collection('users').document('alovelace'), {'born': 1815})
transaction.delete(db.collection('users').document('alovelace'))
transaction.commit()
```

## Running the tests

```bash
poetry install
poetry run pytest
```

## Original Contributors

* [Matt Dowds](https://github.com/mdowds)
* [Chris Tippett](https://github.com/christippett)
* [Anton Melnikov](https://github.com/notnami)
* [Ben Riggleman](https://github.com/briggleman)
* [Steve Atwell](https://github.com/satwell)
* [ahti123](https://github.com/ahti123)
* [Billcountry Mwaniki](https://github.com/Billcountry)
* [Lucas Moura](https://github.com/lsantosdemoura)
* [Kamil Romaszko](https://github.com/kromash)
* [Anna Melnikov](https://github.com/notnami)
* [Carl Chipperfield](https://github.com/carl-chipperfield)
* [Aaron Loo](https://github.com/domanchi)
* [Kristof Krenn](https://github.com/KrennKristof)
* [Ben Phillips](https://github.com/tavva)
* [Rene Delgado](https://github.com/RDelg)
* [klanderson](https://github.com/klanderson)
* [William Li](https://github.com/wli)
* [Ugo Marchand](https://github.com/UgoM)
* [Bryce Thornton](https://github.com/brycethornton)
