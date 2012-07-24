import os
import os.path
import hashlib

def init(dirpath):
    for i in range(16**3):
        os.makedirs(os.path.join(dirpath,
            '{:03x}'.format(i)))

class FileStore(object):
    def __init__(self, dirpath):
        assert dirpath != ""
        self.dirpath = dirpath

    def path_to(self, dhash):
        return os.path.join(self.dirpath,
                os.path.join(dhash[:3],
                    dhash))

    def store(self, data):
        """Returns (datatype, dataid)"""

        sha = hashlib.sha256(data)
        dhash = sha.hexdigest()

        filepath = self.path_to(dhash)
        if os.path.isfile(filepath):
            print("{} already exists".format(dhash))
            return ('flat', dhash)

        with open(filepath, 'wb') as f:
            f.write(data)

        return ('flat', dhash)

    def has(self, datapair):
        return os.path.isfile(self.path_to(datapair[1]))

    def read(self, datapair, size, off):
        assert datapair[0] == 'flat'
        filepath = self.path_to(datapair[1])
        with open(filepath, 'rb') as f:
            f.seek(off)
            return f.read(size)
