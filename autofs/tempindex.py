from __future__ import unicode_literals, print_function, absolute_import

import copy
import shutil
import os.path
import errno
import datetime
import posixpath as ppath

from autofs import fsindex, filestore


class TempFileEntry(object):
    def __init__(self, name, datapath):
        self.name = name
        self.ftype = fsindex.FILE
        self.mtime = datetime.datetime.utcnow()

        self.datapath = datapath
        self.istemp = True

    @property
    def size(self):
        return os.path.getsize(self.datapath)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            pass
        else: raise


class TempIndex(object):
    def __init__(self, temp_root, vi):
        self.vi = vi
        self.temp_root = temp_root
        self.new_entries = {}
        # TODO
        self.renames = []

    def is_changed(self, path):
        return path in self.new_entries

    def lookup(self, path):
        if not self.is_changed(path):
            return self.vi.lookup(path)

        return self.new_entries[path]


    # Make modifications to files/dirs
    def modify_dir(self, dirpath, new_entry):
        assert len(dirpath) > 0

        if not self.is_changed(dirpath):
            self.modify(dirpath, None)

        dir_entry = self.lookup(dirpath)
        assert dir_entry.ftype == fsindex.DIR
        dir_entry.items[new_entry.name] = new_entry

        #if dirpath != '/':
        #    self.modify_dir(ppath.dirname(dirpath), dir_entry)

    def get_datapath(self, filepath):
        datapath = os.path.abspath(os.path.join(self.temp_root, filepath.lstrip('/')))
        print('datapath: {}'.format(datapath))
        return datapath


    def modify(self, path, fs):
        """Returns the new entry"""
        if self.is_changed(path):
            return self.lookup(path)

        old_entry = self.vi.lookup(path)
        if old_entry.ftype == fsindex.DIR:
            new_entry = fsindex.DirEntry(old_entry.name)
            new_entry.items = copy.copy(old_entry.items)
        else:
            assert fs is not None

            # Extract file to temp dir
            datapath = self.get_datapath(path)
            mkdir_p(os.path.dirname(datapath))
            with open(datapath, 'wb') as f:
                with fs.blockfile(old_entry.block_id) as bf:
                    shutil.copyfileobj(bf, f)

            new_entry = TempFileEntry(old_entry.name, datapath)

        self.new_entries[path] = new_entry
        if path != '/':
            self.modify_dir(ppath.dirname(path), new_entry)

        return new_entry

    def create(self, path, ftype):
        bname = ppath.basename(path)
        if ftype == fsindex.DIR:
            new_entry = fsindex.DirEntry(bname)
        else:
            datapath = self.get_datapath(path)
            mkdir_p(os.path.dirname(datapath))
            with open(datapath, 'wb') as f:
                f.write(b'')

            new_entry = TempFileEntry(bname, datapath)

        self.new_entries[path] = new_entry
        self.modify_dir(ppath.dirname(path), new_entry)

        return new_entry


    def delete(self, path):
        if path == '/':
            raise NotImplementedError()

        def delete_recur(dpath):
            old_entry = self.lookup(dpath)
            if old_entry.ftype == fsindex.DIR:
                for i in old_entry.items:
                    delete_recur(ppath.join(dpath, i))

            self.new_entries[dpath] = None

        delete_recur(path)

        parent_dir = self.modify(ppath.dirname(path), None)
        assert parent_dir is not None
        del parent_dir.items[ppath.basename(path)]


    def finalize(self, target_index, fs):
        """Apply changes from this temp index into target_index (which should be empty). Does not delete the temp dir."""
        # Store temp files into filestore
        entries = list(self.new_entries.items())
        for path, new_entry in entries:
            if new_entry is None:
                continue

            if new_entry.ftype == fsindex.FILE:
                # Transform into an ordinary FileEntry
                block_id = fs.store_file(new_entry.datapath)
                new_entry = fsindex.FileEntry(new_entry.name, block_id, new_entry.size)
                self.new_entries[path] = new_entry
                self.modify_dir(ppath.dirname(path), new_entry)


        # Add entries to target_index
        for path, entry in self.vi.walk():
            if not self.is_changed(path):
                print("Inserting (old) {}".format(path))
                target_index.insert(path, entry)
            else:
                new_entry = self.new_entries[path]
                if new_entry is None:
                    continue
                print("Inserting (modified) {}".format(path))
                target_index.insert(path, new_entry)

        # New entries
        for path, new_entry in sorted(self.new_entries.items(), key=lambda pair: pair[0]):
            if new_entry is None:
                continue
            print("Inserting (new) {}".format(path))
            target_index.insert(path, new_entry)

