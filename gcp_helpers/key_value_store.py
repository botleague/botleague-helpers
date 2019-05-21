from __future__ import print_function

import gcp_helpers.constants as c


class SimpleKeyValueStore:
    def get(self, key):
        raise NotImplementedError()

    def set(self, key, value):
        raise NotImplementedError()


class SimpleKeyValueStoreFirestore(SimpleKeyValueStore):
    def __init__(self):
        self.collection_name = 'simple_key_value_store'
        from firebase_admin import firestore
        self.kv = firestore.client().collection(self.collection_name)

    def get(self, key):
        value = self.kv.document(self.collection_name).get().to_dict()[key]
        return value

    def set(self, key, value):
        self.kv.document(self.collection_name).set({key: value})


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
