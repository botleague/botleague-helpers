import random
import string
import sys

from loguru import logger as log

from botleague_helpers.db import get_db
from botleague_helpers import fan_in

TEST_DB_NAME = 'test_db_delete_me'


def test_compare_and_set_live_db():
    db = get_db(TEST_DB_NAME, force_firestore_db=True)
    db.set('yo', 1)
    should_be_false = db.compare_and_swap('yo', 2, 2)
    assert should_be_false is False
    assert db.get('yo') == 1
    db.delete_all_test_data()


def test_namespace_live_db():
    rand_str_get_set(collection_name='')
    rand_str_get_set(collection_name=TEST_DB_NAME)


def test_fan_in():
    test_id = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(32))
    db_name = f'test_data_fan_in_can_delete_{test_id}'
    db = get_db(db_name, force_firestore_db=True)
    fan_in.create_fan_in(test_id, db=db)
    a = True
    b = False
    def ready_fn():
        return a and b
    def reduce_fn():
        return 'asdf'

    result = fan_in.fan_in(test_id, ready_fn, reduce_fn, db, max_attempts=1)
    assert not result
    db.set(test_id, fan_in.REVIEWING)
    result = fan_in.fan_in(test_id, ready_fn, reduce_fn, db, max_attempts=1)
    assert not result
    db.set(test_id, fan_in.WAITING)
    b = True
    result = fan_in.fan_in(test_id, ready_fn, reduce_fn, db, max_attempts=1)
    assert result.reduce_result == 'asdf'
    result = fan_in.fan_in(test_id, ready_fn, reduce_fn, db, max_attempts=1)
    assert not result



def watch_collection_play():
    db = get_db(TEST_DB_NAME, force_firestore_db=True)

    # Create a callback on_snapshot function to capture changes
    def on_snapshot(col_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == 'ADDED':
                print(u'New city: {}'.format(change.document.id))
            elif change.type.name == 'MODIFIED':
                print(u'Modified city: {}'.format(change.document.id))
            elif change.type.name == 'REMOVED':
                print(u'Removed city: {}'.format(change.document.id))

    col_query = db.collection.where('b', '>=', '')

    # Watch the collection query
    query_watch = col_query.on_snapshot(on_snapshot)

    db.set('a3', {'b': 'c'})
    db.set('a4', {'b': 'd'})
    input('press any key to exit')
    db.delete_all_test_data()


def rand_str_get_set(collection_name):
    db = get_db(collection_name,
                force_firestore_db=True)
    rand_str = 'test_data_can_delete_' + ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(12))
    db.set(rand_str, rand_str)
    assert db.get(rand_str) == rand_str
    db.delete_all_test_data()


def run_all(current_module):
    log.info('Running all tests')
    num = 0
    for attr in dir(current_module):
        if attr.startswith('test_'):
            num += 1
            log.info('Running ' + attr)
            getattr(current_module, attr)()
            log.success(f'Test: {attr} ran successfully')
    return num


def main():
    test_module = sys.modules[__name__]
    if len(sys.argv) > 1:
        test_case = sys.argv[1]
        log.info('Running ' + test_case)
        getattr(test_module, test_case)()
        num = 1
        log.success(f'{test_case} ran successfully!')
    else:
        num = run_all(test_module)
    log.success(f'{num} tests ran successfully!')


if __name__ == '__main__':
    main()
    # TODO: Put test data in a separate project
