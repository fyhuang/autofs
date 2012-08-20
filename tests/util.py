from __future__ import unicode_literals, print_function, absolute_import

import os
import os.path
import errno
import tempfile
from contextlib import contextmanager

from autofs import instance
from autofs.cmd import cmd_bundle

class FileMaker(object):
    def __init__(self, root_path):
        self.root_path = root_path

    def mkdir_p(self, path):
        # From <http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python>
        fullpath = os.path.join(self.root_path, path)
        try:
            os.makedirs(fullpath)
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                pass
            else: raise

    def make_file(self, path, data):
        self.mkdir_p(os.path.dirname(path))

        with open(os.path.join(self.root_path, path), 'wb') as f:
            f.write(data)

@contextmanager
def file_maker(root_path):
    yield FileMaker(root_path)


def create_test_instance():
    root_path = tempfile.mkdtemp('autofs')
    inst_path = os.path.join(root_path, 'inst')
    os.mkdir(inst_path)
    inst = instance.Instance.create(inst_path)

    files1_path = os.path.join(root_path, 'files1')
    with file_maker(files1_path) as fm:
        fm.make_file('test1', b'hello world')
        fm.make_file('test2', b'hello world 2')

        fm.make_file('dir_a/test_file', 'abc')
        fm.make_file('dir_a/dir_b/test_file', 'abcd')
        fm.mkdir_p('dir_a/dir_c')

    cmd_bundle.bundle(inst, files1_path)

    return root_path, inst
