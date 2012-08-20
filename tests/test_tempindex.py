from __future__ import unicode_literals, print_function, absolute_import

import unittest
import shutil
import os.path

import util
from autofs import tempindex, fsindex

class TestTempIndex(unittest.TestCase):
    def setUp(self):
        self.tempdir, self.inst = util.create_test_instance()
        self.bundle = self.inst.fi.bundles.values()[0]

        self.ti = tempindex.TempIndex(os.path.join(self.tempdir, 'inflight_1'), self.bundle.latest())
        self.bundle.inflight = self.ti


    def test_modify(self):
        ti = self.ti
        self.assertEqual(ti.is_changed('/test1'), False)

        # Modify
        test1_entry = ti.modify('/test1', self.inst.fs)
        self.assertTrue(ti.is_changed('/test1'))
        self.assertTrue(ti.is_changed('/'))

        with open(test1_entry.datapath, 'rb') as f:
            self.assertEqual(f.read(), b'hello world')
        self.assertEqual(test1_entry.size, 11)

    def test_create(self):
        ti = self.ti
        ti.create('/test3', fsindex.FILE)

        dir_entry = ti.lookup('/')
        self.assertTrue('test3' in dir_entry.items)

        for path, entry in dir_entry.items.items():
            print(path, entry)


    def test_delete(self):
        ti = self.ti
        ti.delete('/test1')
        self.assertTrue(ti.is_changed('/test1'))
        self.assertTrue(ti.lookup('/test1') is None)

        ti.delete('/dir_a/dir_b')
        self.assertTrue(ti.is_changed('/dir_a'))
        self.assertTrue(ti.lookup('/dir_a') is not None)
        self.assertTrue(ti.lookup('/dir_a/dir_c') is not None)
        self.assertTrue(ti.lookup('/dir_a/test_file') is not None)

        self.assertTrue(ti.lookup('/dir_a/dir_b') is None)
        self.assertTrue(ti.lookup('/dir_a/dir_b/test_file') is None)


    def test_finalize(self):
        ti = self.ti

        ti.delete('/dir_a/dir_b')
        new_entry = ti.create('/test3', fsindex.FILE)
        with open(new_entry.datapath, 'wb') as f:
            f.write(b'new test file')

        b = self.bundle.minimal_copy(self.inst.fs)
        vi = b.latest()
        self.assertTrue(vi.lookup('/test3') is not None)

        new_entry = vi.lookup('/test3')
        with self.inst.fs.blockfile(new_entry.block_id) as f:
            print(f.read())
        self.assertEqual(new_entry.block_id, '7383075ee93d980b9d38a1a4408e6e758770c59f9ebe0a8f9d907a272a5cfaf5')



    def tearDown(self):
        shutil.rmtree(self.tempdir)

if __name__ == "__main__":
    unittest.main()
