from __future__ import print_function

import struct
from hashlib import md5

from blob_store import BlobStore

try:
    import cPickle as pickle
except ModuleNotFoundError:
    import pickle

QQ = 'QQ'
idx_record_len = struct.calcsize(QQ)


def myhash(obj):
    string = pickle.dumps(obj)
    return struct.unpack('Q', md5(string).digest()[:8])[0]


class DiskKvStore(object):
    def __init__(self, idxfile, blobfile):
        self._idxfile, self._blobstore = idxfile, BlobStore(blobfile)
        idxfile_size = idxfile.seek(0, 2)
        if idxfile_size == 0:
            idxfile.seek(0, 0)
            idxfile.write(b'\x00' * (idx_record_len * 8))
            idxfile_size = idxfile.seek(0, 2)
        self._idxsize = int(idxfile_size / idx_record_len)
        self._empty_slots = self._idxsize
        idxfile.seek(0, 0)
        while True:
            raw_data = idxfile.read(idx_record_len)
            if not raw_data:
                break
            data = struct.unpack(QQ, raw_data)
            if data != (0, 0):
                self._decrement_empty_slot()

    def __contains__(self, key):
        i, pkey = self._find_key(key)
        return pkey is not None

    def __delitem__(self, key):
        i, pkey = self._find_key(key)
        if pkey == key:
            k, v = self._get_data_location(i)
            self._idxfile.seek(idx_record_len * i, 0)
            self._idxfile.write(struct.pack(QQ, 0, 0))
            self._idxfile.flush()
            self._blobstore.delete(k)
            self._blobstore.delete(v)
            self._empty_slots += 1
            return

        raise KeyError()

    def __getitem__(self, key):
        assert key is not None
        i, pkey = self._find_key(key)
        if pkey == key:
            return self._read_value(i)

        raise KeyError()

    def __setitem__(self, key, value):
        new = False
        i, pkey = self._find_key(key)
        if pkey == key:
            # update
            v_ptr = self._add_blob(value)
            data_location = self._get_data_location(i)[0], v_ptr
        else:
            # new
            data_location = self._add_blob(key), self._add_blob(value)
            new = True
        self._idxfile.seek(idx_record_len * i, 0)
        self._idxfile.write(struct.pack(QQ, *data_location))
        self._idxfile.flush()
        if new:
            self._decrement_empty_slot()

    def _find_key(self, key):
        assert key is not None
        key_hash = myhash(key)
        perturb = key_hash
        i = key_hash % self._idxsize
        pkey = self._read_key(i)
        while pkey is not None and key != pkey:
            i = (5 * i + perturb + 1) % self._idxsize
            perturb >>= 5
            pkey = self._read_key(i)
        return i, pkey

    def _read_index(self, i, kv):
        data_location = self._get_data_location(i)
        if data_location == (0, 0):
            return None
        ret = self._read_blob(data_location[kv])
        return ret

    def _read_key(self, i):
        return self._read_index(i, 0)

    def _read_value(self, i):
        return self._read_index(i, 1)

    def _read_keyvalue(self, i):
        data_location = self._get_data_location(i)
        if data_location == (0, 0):
            return None, None
        ret = self._read_blob(data_location[0]), self._read_blob(data_location[1])
        return ret

    def _get_data_location(self, i):
        self._idxfile.seek(idx_record_len * i, 0)
        data_location = struct.unpack(QQ, self._idxfile.read(idx_record_len))
        return data_location

    def _add_blob(self, value):
        return self._blobstore.add(value)

    def _read_blob(self, start):
        return self._blobstore.get(start)

    def items(self):
        for data_location, _ in self._all_locations():
            yield self._read_blob(data_location[0]), self._read_blob(data_location[1])

    def _all_locations(self, limit=None):
        idx = 0
        while True:
            self._idxfile.seek(idx * idx_record_len, 0)
            raw_data = self._idxfile.read(idx_record_len)
            if not raw_data or (limit is not None and limit < 1):
                break
            if limit is not None:
                limit -= 1
            data_location = struct.unpack(QQ, raw_data)
            if data_location != (0, 0):
                yield data_location, idx
            idx += 1

    def __iter__(self):
        for data_location, _ in self._all_locations():
            yield self._read_blob(data_location[0])

    def _decrement_empty_slot(self):
        self._empty_slots -= 1
        if self._empty_slots == 0:
            # double the file size
            self._idxfile.seek(0, 2)
            self._idxfile.write(b'\x00' * (idx_record_len * self._idxsize * 2))
            self._idxfile.flush()
            self._idxsize *= 2
            # rehash the keys with the new lenght
            for loc, idx in self._all_locations(self._idxsize // 2):
                ptr_k, ptr_v = loc
                key = self._read_blob(ptr_k)
                key_hash = myhash(key)
                perturb = key_hash
                i = key_hash % self._idxsize
                location = self._get_data_location(i + self._idxsize // 2)
                while location != (0, 0):
                    i = (5 * i + perturb + 1) % self._idxsize
                    perturb >>= 5
                    location = self._get_data_location(i + self._idxsize // 2)
                self._idxfile.seek(idx_record_len * (i + self._idxsize // 2), 0)
                self._idxfile.write(struct.pack(QQ, *loc))
                self._idxfile.flush()
            # copy all to the begginning of the file and remove offset
            ptr = 0
            self._idxfile.seek(idx_record_len * (self._idxsize // 2), 0)
            for data in iter(lambda: self._idxfile.read(idx_record_len), b''):
                self._idxfile.seek(idx_record_len * ptr, 0)
                self._idxfile.write(data)
                ptr += 1
                self._idxfile.seek(idx_record_len * (ptr + self._idxsize // 2), 0)
            self._idxfile.truncate(idx_record_len * self._idxsize)
            self._idxfile.flush()
            self._empty_slots = self._idxsize // 2


def main():
    for filename in ('idx', 'datafile'):
        # if not os.path.exists(filename):
        open(filename, 'wb').close()

    with open('idx', 'r+b') as idx, open('datafile', 'r+b') as blob:
        dks = DiskKvStore(idx, blob)

        for k, v in dks.items():
            print('{}:\t{}'.format(k, v))

        dks['test'] = 'string'
        dks['test2'] = 'the other string'

        assert dks['test'] == 'string'

        del dks['test']

        assert 'test' not in dks

        for k in dks:
            print(k)

        dks['test1'] = 1
        dks['test2'] = 2
        dks['test3'] = 3
        dks['test4'] = 4
        dks['test5'] = 5
        dks['test6'] = 6
        dks['test7'] = 7
        dks['test8'] = 8

        dks['test9'] = 9

        del dks

        dks2 = DiskKvStore(idx, blob)
        for k, v in dks2.items():
            print('{}:\t{}'.format(k, v))


if __name__ == "__main__":
    main()
