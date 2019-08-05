import random
import string

from botleague_helpers.key_value_store import get_key_value_store

TEST_DB_NAME = 'test_db_delete_me'


def test_compare_and_set_live_db():
    kv = get_key_value_store(TEST_DB_NAME, force_firestore_db=True)
    kv.set('yo', 1)
    should_be_false = kv.compare_and_swap('yo', 2, 2)
    assert should_be_false is False
    assert kv.get('yo') == 1


def test_namespace_live_db():
    rand_str_get_set(collection_name='')
    rand_str_get_set(collection_name=TEST_DB_NAME)


def watch_collection_play():
    kv = get_key_value_store(TEST_DB_NAME, force_firestore_db=True)

    # Create a callback on_snapshot function to capture changes
    def on_snapshot(col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == 'ADDED':
                print(u'New city: {}'.format(change.document.id))
            elif change.type.name == 'MODIFIED':
                print(u'Modified city: {}'.format(change.document.id))
            elif change.type.name == 'REMOVED':
                print(u'Removed city: {}'.format(change.document.id))

    col_query = kv.collection.where('b', '>=', '')

    # Watch the collection query
    query_watch = col_query.on_snapshot(on_snapshot)

    kv.set('a3', {'b': 'c'})
    kv.set('a4', {'b': 'd'})
    input('press any key to exit')


def rand_str_get_set(collection_name):
    kv = get_key_value_store(collection_name,
                             force_firestore_db=True)
    rand_str = 'test_data_can_delete_' + ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(12))
    kv.set(rand_str, rand_str)
    assert kv.get(rand_str) == rand_str


if __name__ == '__main__':
    test_compare_and_set_live_db()
    test_namespace_live_db()
    # watch_collection_play()
    # TODO: Put test data in a separate project
    # TODO: Clean things up, very carefully
