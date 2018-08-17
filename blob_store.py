import pickle
import struct
import contextlib

# header
# len                 Q  64 bit unsigned
# additional to skip  Q  64 bit unsigned
# deleted             ?  bool
from tempfile import SpooledTemporaryFile

HEADER_FORMAT = '<QQ?'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


@contextlib.contextmanager
def ignored(*exceptions):
    try:
        yield
    except exceptions:
        pass


class BlobStore(object):
    def __init__(self, fp):
        self._fp = fp
        self._holes = {ptr: l1 + l2 for ptr, l1, l2, deleted in self.headers() if deleted}

    def _append(self, obj):
        encoded_obj = pickle.dumps(obj)
        length = len(encoded_obj)
        self._fp.write(struct.pack(HEADER_FORMAT, length, 0, False))
        self._fp.write(encoded_obj)
        self._fp.flush()

    def add(self, obj):
        encoded_obj = pickle.dumps(obj)
        length = len(encoded_obj)
        ptr, previous_len = self._find_space(length)

        self._fp.seek(ptr, 0)
        self._fp.write(struct.pack(HEADER_FORMAT, length, previous_len - length, False))
        self._fp.write(encoded_obj)
        self._fp.flush()
        with ignored(KeyError):
            del self._holes[ptr]
        return ptr

    def get(self, ptr):
        self._fp.seek(ptr, 0)
        length, _, deleted = struct.unpack(HEADER_FORMAT, self._fp.read(HEADER_SIZE))
        if deleted:
            raise ValueError('object was delete')
        obj = pickle.loads(self._fp.read(length))
        return obj

    def delete(self, ptr):
        self._fp.seek(ptr, 0)
        length, add, deleted = struct.unpack(HEADER_FORMAT, self._fp.read(HEADER_SIZE))
        self._fp.seek(ptr, 0)
        self._fp.write(struct.pack(HEADER_FORMAT, length + add, 0, True))
        self._fp.flush()
        self._holes[ptr] = length + add

    def _find_space(self, length):
        for ptr, curr_length in self._holes.items():
            if curr_length >= length:
                return ptr, curr_length
        ret = self._fp.seek(0, 2)
        if ret is None:
            ret = 0
        return ret, length

    def headers(self):
        self._fp.seek(0, 0)
        while True:
            raw_data = self._fp.read(HEADER_SIZE)
            if not raw_data:
                break
            curr_length, additionals_bytes, deleted = struct.unpack(HEADER_FORMAT, raw_data)
            yield self._fp.tell() - HEADER_SIZE, curr_length, additionals_bytes, deleted
            self._fp.seek(curr_length + additionals_bytes, 1)

    def __iter__(self):
        self._fp.seek(0, 0)
        while True:
            raw_data = self._fp.read(HEADER_SIZE)
            if not raw_data:
                break
            curr_length, additionals_bytes, deleted = struct.unpack(HEADER_FORMAT, raw_data)
            if not deleted:
                obj = pickle.loads(self._fp.read(curr_length))
                yield obj
                self._fp.seek(additionals_bytes, 1)
            else:
                self._fp.seek(curr_length + additionals_bytes, 1)

    def items(self):
        self._fp.seek(0, 0)
        while True:
            raw_data = self._fp.read(HEADER_SIZE)
            if not raw_data:
                break
            curr_length, additionals_bytes, deleted = struct.unpack(HEADER_FORMAT, raw_data)
            obj = pickle.loads(self._fp.read(curr_length))
            yield self._fp.tell() - HEADER_SIZE - curr_length, curr_length, additionals_bytes, deleted, obj
            self._fp.seek(additionals_bytes, 1)

    def vacuum(self):
        with SpooledTemporaryFile() as ftemp:
            bstemp = BlobStore(ftemp)
            for x in self:
                bstemp._append(x)
            ftemp.seek(0, 0)
            self._fp.seek(0, 0)
            for data in iter(lambda: ftemp.read(4096), b''):
                self._fp.write(data)
            self._fp.truncate()
            self._fp.flush()
            self._holes.clear()


def main():
    open('blobfile', 'wb').close()

    with open('blobfile', 'r+b') as blob:
        bs = BlobStore(blob)
        ptr = bs.add('stringa')
        ptr2 = bs.add('stringa2')
        ptr3 = bs.add('stringa3')

        assert 'stringa' == bs.get(ptr)
        assert 'stringa2' == bs.get(ptr2)
        assert 'stringa3' == bs.get(ptr3)

        bs.delete(ptr2)

        try:
            bs.get(ptr2)
            assert False
        except ValueError:
            pass

        ptr4 = bs.add('4')
        assert '4' == bs.get(ptr4)
        assert ptr4 < ptr3
        for x in bs.items():
            print(x)
        bs.delete(ptr4)

        ptr5 = bs.add('55')
        assert '55' == bs.get(ptr5)
        assert ptr5 == ptr4
        bs.delete(ptr5)

        for x in bs.headers():
            print(x)

        for x in bs:
            print(x)

        assert 'stringa' in bs
        assert '55' not in bs

        print('prevacuum')
        for x in bs.items():
            print(x)

        bs.vacuum()

        print('postvacuum')
        for x in bs.items():
            print(x)


if __name__ == "__main__":
    main()
