import unittest
from autofs import fsindex

class TestFilesystemIndex(unittest.TestCase):
    def setUp(self):
        self.fsi = fsindex.FilesystemIndex()
        self.test1 = self.fsi.newbundle('test1')

    def test_newbundle(self):
        test2 = self.fsi.newbundle('test2')
        self.assertEqual(len(self.fsi.bundles), 2)

    def test_minimalcopy(self):
        copy = self.fsi.minimal_copy()
        for bid, b in copy.bundles.items():
            self.assertEqual(len(b.indexes), 2)

if __name__ == "__main__":
    unittest.main()
