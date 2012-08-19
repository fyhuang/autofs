import unittest
import tempfile
import shutil

import gevent
from autofs import filestore

class TestPeerConnection(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp('autofs')
        print("Temp dir: {}".format(self.tempdir))
        filestore.init(self.tempdir)
        self.fs = filestore.FileStore(self.tempdir)

    def test_store(self):
        data1 = b'hello world'
        data2 = b'this is a test'
        data3 = b''

        bid1 = self.fs.store(data1)
        self.assertEqual(bid1, 'b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9')
        bid2 = self.fs.store(data1)
        self.assertEqual(bid1, bid2)

        bid2 = self.fs.store(data2)
        self.assertTrue(bid1 != bid2)

        bid3 = self.fs.store(data3)
        self.assertEqual(bid3, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')

        self.assertEqual(self.fs.blockdata(bid1), data1)
        self.assertEqual(self.fs.blockdata(bid2), data2)
        self.assertEqual(self.fs.blockdata(bid3), data3)

    def test_cache(self):
        data1 = b'test1'
        bid = filestore.get_block_id(data1)

        ret = self.fs.cache(bid, data1)
        self.assertTrue(ret)

        self.assertTrue(self.fs.get_cached(bid) is not None)


    def tearDown(self):
        shutil.rmtree(self.tempdir)

if __name__ == "__main__":
    unittest.main()
