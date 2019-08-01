import random
import string

from botleague_helpers.key_value_store import get_key_value_store


def test_namespace_live_db():
    rand_str_get_set(collection_name='')
    rand_str_get_set(collection_name='test_db')


def rand_str_get_set(collection_name):
    kv = get_key_value_store(collection_name,
                             test_remote_db=True)
    rand_str = 'test_data_can_delete_' + ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(12))
    kv.set(rand_str, rand_str)
    assert kv.get(rand_str) == rand_str


if __name__ == '__main__':
    test_namespace_live_db()
