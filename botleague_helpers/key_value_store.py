from __future__ import print_function

from botleague_helpers.config import blconfig
from botleague_helpers.config import get_test_name_from_callstack


class SimpleKeyValueStore:
    collection_name: str = 'simple_key_value_store'

    def get(self, key):
        raise NotImplementedError()

    def set(self, key, value):
        raise NotImplementedError()


class SimpleKeyValueStoreFirestore(SimpleKeyValueStore):
    def __init__(self):
        from firebase_admin import firestore
        blconfig.ensure_firebase_initialized()
        self.kv = firestore.client().collection(self.collection_name)

    def get(self, key):
        value = self.kv.document(key).get().to_dict()
        if key in value and len(value) == 1:
            # Value is just a field
            value = value[key]
        return value

    def set(self, key, value):
        if not isinstance(value, dict):
            # Value is just a field
            self.kv.document(key).set({key: value})
        else:
            # Value is a collection
            self.kv.document(key).set(value)


class SimpleKeyValueStoreLocal(SimpleKeyValueStore):
    def __init__(self):
        self.kv = {}

    def get(self, key):
        return self.kv[key]

    def set(self, key, value):
        self.kv[key] = value


def get_key_value_store() -> SimpleKeyValueStore:
    test_name = get_test_name_from_callstack()
    if test_name:
        print('We are in a test, %s, so not using Firestore' % test_name)
        return SimpleKeyValueStoreLocal()
    elif blconfig.should_use_firestore:
        print('Using Firestore backed key value store')
        return SimpleKeyValueStoreFirestore()
    else:
        print('SHOULD_USE_FIRESTORE is false, so not using Firestore')
        return SimpleKeyValueStoreLocal()
