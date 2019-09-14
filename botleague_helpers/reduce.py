from box import Box
from typing import List, Union
import time

from loguru import logger as log

from botleague_helpers.db import get_db

WAITING = 'waiting'
REVIEWING = 'reviewing'
FINISHED = 'finished'


def create_reduce(reduce_id, db=None):
    """
    Setup reduce (see below). This should be done before
    fanning out / mapping.
    """
    db = db or get_reduce_db()
    db.set(reduce_id, WAITING)


def try_reduce_async(reduce_id: str, ready_fn: callable, reduce_fn: callable,
                     db=None, max_attempts=-1) -> Union[bool, Box]:
    """
    Concurrent-safe execution of reduce_fn when ready_fn is True.

    :param reduce_id: Unique string representing your reduce operation
    :param ready_fn: Function that returns True when reduce items are ready
    :param reduce_fn: Function that executes reduce, i.e. reduce
    :param db: [Optional] DB to use (i.e. for testing)
    :param max_attempts [Optional] For testing - number of times to sleep
    while waiting for result. -1 means to wait until current reviewer is done,
    which is always what you want outside of tests.
    :return: Returns Box(reduce_result=reduce_fn()), else False
    """

    def become_reviewer():
        waiting = db.compare_and_swap(reduce_id, WAITING, REVIEWING)
        finished = db.compare_and_swap(reduce_id, FINISHED, REVIEWING)
        return waiting or finished

    # If not complete, become reviewer and mark complete or not
    db = db or get_reduce_db()

    if not db.get(reduce_id):
        raise RuntimeError(f'Reduce collection {reduce_id} does not exist')

    attempts = 0

    # CAS that we are reviewing to prevent other reviewers
    while not become_reviewer() and (max_attempts == -1 or
                                     attempts < max_attempts):
        # Note: max attempts is just for testing.

        # If CAS fails, wait for other reviewer finish
        time.sleep(0.1)
        attempts += 1
        if attempts >= max_attempts:
            return False

    # We are reviewer, check to make sure previous reviewers did not finish
    # already.
    if db.get(reduce_id) == FINISHED:
        return False
    else:
        # We are the reviewer, reduce if we are ready
        if ready_fn():
            ret = reduce_fn()
            return Box(reduce_result=ret)
        else:
            # Not ready, don't reduce
            return False


def get_reduce_db():
    return get_db('botleague_reduce')

