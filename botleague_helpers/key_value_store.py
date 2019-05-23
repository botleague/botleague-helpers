from __future__ import print_function

import botleague_helpers.constants as c


class SimpleKeyValueStore:
    collection_name: str = 'simple_key_value_store'

    def get(self, key):
        raise NotImplementedError()

    def set(self, key, value):
        raise NotImplementedError()


class SimpleKeyValueStoreFirestore(SimpleKeyValueStore):
    def __init__(self):
        from firebase_admin import firestore
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
    if c.SHOULD_USE_FIRESTORE:
        return SimpleKeyValueStoreFirestore()
    else:
        return SimpleKeyValueStoreLocal()
