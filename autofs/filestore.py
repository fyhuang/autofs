import os
import os.path
import shutil
import hashlib
import pickle
import collections
import contextlib

MAX_CACHE = 256

def init(dirpath):
    for i in range(16**3):
        os.makedirs(os.path.join(dirpath,
            '{:03x}'.format(i)))

def get_block_id(data):
    sha = hashlib.sha256(data)
    block_id = sha.hexdigest()
    return block_id

def get_block_id_fileobj(f):
    sha = hashlib.sha256()
    while True:
        data = f.read(8192)
        if not data:
            break
        sha.update(data)
    return sha.hexdigest()

class FileStore(object):
    def __init__(self, dirpath):
        assert dirpath != ""
        self.dirpath = dirpath
        self.stored_bundles = set()
        self.cached_objects = collections.deque()
        self.refcounts = {}

    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    def path_to_block(self, dhash):
        return os.path.join(self.dirpath,
                os.path.join(dhash[:3],
                    dhash))


    # Caching
    def cache(self, block_id, data):
        if not block_id in self:
            self.store(data)

            self.cached_objects.append(block_id)
            self.addref(block_id)

            if len(self.cached_objects) > MAX_CACHE:
                bid = self.cached_objects.popleft()
                self.decref(bid)

            return True
        return False

    def get_cached(self, block_id):
        for bid in self.cached_objects:
            if bid == block_id:
                return bid
        return None
            

    # Reference counting
    def addref(self, bid):
        self.refcounts[bid] += 1
    def decref(self, bid):
        self.refcounts[bid] -= 1
        assert self.refcounts[bid] >= 0

    def evict_bundle(self, bundle):
        for path in bundle.walk():
            pass

    def delete_unused(self):
        pass


    # Storage
    def __contains__(self, block_id):
        return os.path.isfile(self.path_to_block(block_id))

    def store(self, data, block_id=None):
        """Returns block ID"""

        if block_id is None:
            block_id = get_block_id(data)

        filepath = self.path_to_block(block_id)
        if block_id in self:
            print("{} already exists".format(block_id))
            return block_id

        with open(filepath, 'wb') as f:
            f.write(data)

        self.refcounts[block_id] = 0
        return block_id

    def store_file(self, filename, block_id=None):
        """Returns block ID"""
        if block_id is None:
            with open(filename, 'rb') as f:
                block_id = get_block_id_fileobj(f)

        out_filepath = self.path_to_block(block_id)
        if block_id in self:
            print("{} already exists".format(block_id))
            return block_id

        shutil.copyfile(filename, out_filepath)
        self.refcounts[block_id] = 0
        return block_id

    """def read(self, block_id, size, off):
        filepath = self.path_to_block(block_id)
        with open(filepath, 'rb') as f:
            f.seek(off)
            return f.read(size)"""

    @contextlib.contextmanager
    def blockfile(self, block_id):
        f = open(self.path_to_block(block_id), 'rb')
        yield f
        f.close()
