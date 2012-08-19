import os
import os.path
import uuid
import pickle
import collections

from autofs import fsindex, filestore

FI_PATH = 'index.pkl'
FS_PATH = 'store.pkl'
FS_DIR = 'store'
STATIC_INFO_PATH = "static_info"
PEER_INFO_PATH = "peer_info"


def pickle_load(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

class PeerInfo(object):
    def __init__(self, peerid):
        self.peerid = peerid
        self.last_seen_addr = collections.deque()

    def update_addr(self, addr):
        if addr in self.last_seen_addr:
            return
        if len(self.last_seen_addr) >= 5:
            self.last_seen_addr.popleft()
        self.last_seen_addr.append(addr)

class Instance(object):
    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.fi = fsindex.FilesystemIndex()
        self.fs = filestore.FileStore(self.path_to(FS_DIR))
        self.static_info = {}
        self.peer_info = {}

    def path_to(self, path):
        return os.path.join(self.dirpath, path)

    def get_peer_info(self, peerid):
        if peerid not in self.peer_info:
            self.peer_info[peerid] = PeerInfo(peerid)
        return self.peer_info[peerid]

    @staticmethod
    def create(dirpath, static_info=None):
        print("Creating instance at {}".format(dirpath))
        assert os.path.isdir(dirpath)

        inst = Instance(dirpath)
        filestore.init(inst.path_to(FS_DIR))

        # Generate a UUID
        if static_info is None:
            static_info = {'cluster_id': uuid.uuid1().hex}
        inst.static_info = static_info
        inst.save()

        return Instance.load(dirpath)

    @staticmethod
    def load(dirpath):
        print("Loading instance at {}".format(dirpath))
        inst = Instance(dirpath)
        inst.fi = pickle_load(inst.path_to(FI_PATH))
        inst.fs = pickle_load(inst.path_to(FS_PATH))
        with open(inst.path_to(STATIC_INFO_PATH), "rb") as f:
            inst.static_info = pickle.load(f)
        with open(inst.path_to(PEER_INFO_PATH), "rb") as f:
            inst.peer_info = pickle.load(f)

        return inst

    def save(self):
        print("Saving instance to {}".format(self.dirpath))
        self.fi.save(self.path_to(FI_PATH))
        self.fs.save(self.path_to(FS_PATH))
        with open(self.path_to(STATIC_INFO_PATH), "wb") as f:
            pickle.dump(self.static_info, f)
        with open(self.path_to(PEER_INFO_PATH), "wb") as f:
            pickle.dump(self.peer_info, f)
        

