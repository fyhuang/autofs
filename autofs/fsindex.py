from datetime import datetime
import pickle
import uuid

FILE = 0
DIR = 1

DIFF = 0
FULL = 1

class FileEntry(object):
    def __init__(self, name, datapair, fsize):
        self.name = name
        self.ftype = FILE
        self.ctime = datetime.utcnow()

        self.datapair = datapair
        self.size = fsize

class DirEntry(object):
    def __init__(self, name):
        self.name = name
        self.ftype = DIR
        self.ctime = datetime.utcnow()
        self.items = {}
        
class VersionedIndex(object):
    def __init__(self, version, date):
        self.version = version
        self.date = date

        self.index = DirEntry('/')

    def traverse(self, path):
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


class Bundle(object):
    def __init__(self, bundle_id, name):
        self.bundle_id = bundle_id
        self.name = name

        self.indexes = [] # 0 = oldest, len(indexes)-1 = newest
        ix = VersionedIndex(0, datetime.utcnow())
        self.indexes.append(ix)

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
    def minimal_copy(self):
        b = Bundle(self.bundle_id, self.name)
        b.indexes.append(self.latest())
        return b

class FilesystemIndex(object):
    def __init__(self):
        self.bundles = {}

    @staticmethod
    def load(filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)

    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    def newbundle(self, bname):
        bndid = uuid.uuid1()
        self.bundles[bndid.hex] = Bundle(bndid.hex, bname)
        return self.bundles[bndid.hex]

    def minimal_copy(self):
        fi = FilesystemIndex()
        for bid,b in self.bundles.items():
            fi.bundles[bid] = b.minimal_copy()
        return fi
