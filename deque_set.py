try:
    import cPickle as pickle
except:
    import pickle
import shutil
import os

from collections import deque
from contextlib import contextmanager


@contextmanager
def ignored(*exceptions):
    try:
        yield
    except exceptions:
        pass


class DequeSet(object):
    def __init__(self, size=10, filename=None):
        self._filename = filename
        self._temp_filename = '{0}.tmp'.format(filename)
        self._size = size
        self._q = deque(maxlen=size)
        self._s = set()
        self.load_from_file()

    def load_from_file(self):
        if self._filename:
            with open(self._filename, 'rb') as fileobj:
                with ignored(Exception):
                    self._q, self._s = pickle.load(fileobj)

    def save_to_file(self):
        if self._filename:
            with ignored(Exception):
                with open(self._temp_filename, 'wb') as fileobj:
                    pickle.dump((self._q, self._s), fileobj)
                with ignored(IOError):
                    os.remove(self._filename)
                shutil.move(self._temp_filename, self._filename)

    def add(self, item):
        if item not in self._s:
            self._s.add(item)
            if len(self._q) == self._size:
                in_exit = self._q.popleft()
                self._s.remove(in_exit)
            self._q.append(item)
            self.save_to_file()

    def __contains__(self, item):
        return item in self._s


if __name__ == "__main__":
    # test here
    ds = DequeSet(3)
    ds.add(19)
    ds.add(13)
    ds.add(14)
    ds.add(15)

    assert 19 not in ds
    assert 13 in ds
    assert 14 in ds
    assert 15 in ds

    ds.add('string')

    assert 13 not in ds
    assert 'string' in ds
