from box import Box
from typing import List, Union

import time

from botleague_helpers.db import get_db

WAITING = 'waiting'
REVIEWING = 'reviewing'
FINISHED = 'finished'


def create_fan_in(fan_in_id, db=None):
    """Setup fan in (see below). This should be done before fanning out."""
    db = db or get_fan_in_db()
    db.set(fan_in_id, WAITING)


def fan_in(fan_in_id: str, ready_fn: callable, reduce_fn: callable,
           db=None, max_attempts=-1) -> Union[bool, Box]:
    """
    Concurrent-safe execution of reduce_fn when all in_functions are True.

    :param fan_in_id: Unique string representing your fan in operation
    :param ready_fn: Function that returns True when fan in is ready
    :param reduce_fn: Function that executes fan in, i.e. reduce
    :param db: [Optional] DB to use (i.e. for testing)
    :param max_attempts [Optional] For testing - number of times to sleep
    while waiting for result. -1 means to wait until current reviewer is done,
    which is always what you want outside of tests.
    :return: Returns Box(reduce_result=reduce_fn()), else False
    """

    def become_reviewer():
        waiting = db.compare_and_swap(fan_in_id, WAITING, REVIEWING)
        finished = db.compare_and_swap(fan_in_id, FINISHED, REVIEWING)
        return waiting or finished

    # If not complete, become reviewer and mark complete or not
    db = db or get_fan_in_db()

    if not db.get(fan_in_id):
        raise RuntimeError(f'Fan in collection {fan_in_id} does not exist')

    attempts = 0

    # CAS that we are reviewing to prevent other reviewers
    while not become_reviewer() and (max_attempts == -1 or
                                     attempts < max_attempts):
        # If CAS fails, wait for other reviewer finish
        time.sleep(0.1)
        attempts += 1
        if attempts >= max_attempts:
            return False

    # We are reviewer, check to make sure previous reviewers did not finish
    # already.
    if db.get(fan_in_id) != FINISHED:
        if ready_fn():
            ret = reduce_fn()
            return Box(reduce_result=ret)
        else:
            return False
    else:
        return False


def get_fan_in_db():
    return get_db('botleague_fan_in')

