from __future__ import print_function

from botleague_helpers.config import blconfig
from botleague_helpers.config import get_test_name_from_callstack

DEFAULT_COLLECTION = 'simple_key_value_store'


class SimpleKeyValueStore:
    def get(self, key):
        raise NotImplementedError()

    def set(self, key, value):
        raise NotImplementedError()


class SimpleKeyValueStoreFirestore(SimpleKeyValueStore):
    def __init__(self, collection_name):
        from firebase_admin import firestore
        blconfig.ensure_firebase_initialized()
        collection_name = collection_name or DEFAULT_COLLECTION
        self.kv = firestore.client().collection(collection_name)

    def get(self, key):
        value = self.kv.document(key).get().to_dict() or {}
        if key in value and len(value) == 1:
            # Document just contains the one field with the same name
            # as the document, so just return the value of the field.
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
    def __init__(self, collection_name):
        self.kv = {}

    def get(self, key):
        return self.kv[key]

    def set(self, key, value):
        self.kv[key] = value


def get_key_value_store(
        collection_name: str = DEFAULT_COLLECTION,
        test_remote_db=False) -> SimpleKeyValueStore:
    """

    :param collection_name:
    :param test_remote_db: For special cases where you want to test the db logic
    :return:
    """
    test_name = get_test_name_from_callstack()
    if test_name and not test_remote_db:
        print('We are in a test, %s, so not using Firestore' % test_name)
        return SimpleKeyValueStoreLocal(collection_name)
    elif blconfig.should_use_firestore:
        print('Using Firestore backed key value store')
        return SimpleKeyValueStoreFirestore(collection_name)
    else:
        print('SHOULD_USE_FIRESTORE is false, so not using Firestore')
        return SimpleKeyValueStoreLocal(collection_name)
