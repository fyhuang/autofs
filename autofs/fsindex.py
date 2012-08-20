from __future__ import unicode_literals, print_function, absolute_import

from datetime import datetime
import pickle
import uuid
import posixpath as ppath


FILE = 0
DIR = 1

DIFF = 0
FULL = 1


class FileEntry(object):
    def __init__(self, name, block_id, fsize):
        self.name = name
        self.ftype = FILE
        self.mtime = datetime.utcnow()

        self.block_id = block_id
        self.size = fsize
        self.istemp = False


class DirEntry(object):
    def __init__(self, name):
        self.name = name
        self.ftype = DIR
        self.mtime = datetime.utcnow()
        self.items = {}
        

class VersionedIndex(object):
    def __init__(self, version, date):
        self.version = version
        self.date = date

        self.index = DirEntry('/')
        self.dirs = {}

    def lookup(self, path):
        components = path.strip('/').split('/')
        curr = self.index
        for c in components:
            if c == '':
                continue

            if curr.ftype != DIR:
                return None
            try:
                curr = curr.items[c]
            except KeyError:
                return None
        return curr

    def walk(self, path='/'):
        def walk_rec(entry):
            assert entry.ftype == DIR
            for k,v in entry.items.items():
                yield (path + k,v)

                if v.ftype == DIR:
                    for k1,v1 in walk_rec(v):
                        yield (path + k + k1,v1)

        yield (path, self.lookup(path))
        for k,v in walk_rec(self.lookup(path)):
            yield (k,v)

    def rebuild_dirs(self):
        for path, entry in self.walk():
            if entry.ftype == DIR:
                self.dirs[path] = entry

    def insert(self, path, entry):
        if path == '/':
            assert entry.ftype == DIR
            self.index = entry
        else:
            assert ppath.basename(path) == entry.name

            parent = self.lookup(ppath.dirname(path))
            assert parent.ftype == DIR

            parent.items[ppath.basename(path)] = entry


class Bundle(object):
    def __init__(self, bundle_id, name):
        self.bundle_id = bundle_id
        self.name = name

        self.indexes = [] # 0 = oldest, len(indexes)-1 = newest
        ix = VersionedIndex(0, datetime.utcnow())
        self.indexes.append(ix)

        self.inflight = None

    """Retrieves the latest version of the file index"""
    def latest(self):
        return self.indexes[-1]

    """Generates a new version of the index and returns it"""
    def newindex(self):
        ix = VersionedIndex(self.latest().version + 1, datetime.utcnow())
        self.indexes.append(ix)
        return ix

    """Returns a copy of this bundle with only the latest
    index (and the starting empty index)"""
    def minimal_copy(self, filestore):
        # Finalize the inflight data
        if self.inflight is not None:
            new_index = self.newindex()
            self.inflight.finalize(new_index, filestore)
            self.inflight = None

        b = Bundle(self.bundle_id, self.name)
        b.indexes.append(self.latest())
        return b


class FilesystemIndex(object):
    def __init__(self):
        self.bundles = {}

    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    def newbundle(self, bname):
        bndid = uuid.uuid1()
        self.bundles[bndid.hex] = Bundle(bndid.hex, bname)
        return self.bundles[bndid.hex]

    def minimal_copy(self, filestore):
        fi = FilesystemIndex()
        for bid,b in self.bundles.items():
            fi.bundles[bid] = b.minimal_copy(filestore)
        return fi
