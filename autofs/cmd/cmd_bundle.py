from __future__ import unicode_literals, print_function, absolute_import

import os.path

from autofs import fsindex


def bundle(inst, dirpath):
    bname = os.path.basename(dirpath)
    print("Creating new bundle \"{}\"".format(bname))

    bundle = inst.fi.newbundle(bname)
    index = bundle.newindex()

    for root, dirs, files in os.walk(dirpath):
        relroot = root[len(dirpath):]
        root_entry = index.lookup(relroot)
        assert root_entry.ftype == fsindex.DIR

        for d in dirs:
            # Create direntry
            print("Creating dir {}".format(os.path.join(relroot, d)))
            name = os.path.basename(d)
            entry = fsindex.DirEntry(name)
            root_entry.items[name] = entry

        for fn in files:
            print("Creating file {}".format(os.path.join(relroot, fn)))
            name = os.path.basename(fn)
            full_fn = os.path.join(root, fn)
            block_id = inst.fs.store_file(full_fn)
            entry = fsindex.FileEntry(name, block_id, os.path.getsize(full_fn))
            root_entry.items[name] = entry

    index.rebuild_dirs()
