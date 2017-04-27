import time

from datetime import datetime, timedelta


class HashableDict(dict):
    """A hashable dict. Useful when you want to pass a dict to a memoized function."""

    def __hash__(self):
        return hash(frozenset(self))


def merge_dicts(*ds):
    """Merge a variable number of dicts, from right to left."""
    to_d = ds[0].copy()

    for from_d in ds[1:]:
        to_d.update(from_d)

    return to_d


def await_until(pred, timeout=30, interval=5):
    """Wait, retrying a predicate until it is True, or the timeout value has been exceeded."""
    finish = datetime.now() + timedelta(seconds=timeout)

    while True:
        result = pred()

        if result:
            break

        if datetime.now() >= finish:
            break

        time.sleep(interval)

    return result
