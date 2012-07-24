import os.path

from autofs import fsindex


def bundle(inst, dirpath):
    bname = os.path.basename(dirpath)
    print("Creating new bundle {}".format(bname))

    bundle = inst.fi.newbundle(bname)
    index = bundle.newindex()

    for root, dirs, files in os.walk(dirpath):
        relroot = root[len(dirpath):]
        root_entry = index.traverse(relroot)

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
            try:
                with open(full_fn, 'rb') as f:
                    data = f.read()
            except IOError:
                print("Error while reading file {}".format(fn))
                continue

            datapair = inst.fs.store(data)
            entry = fsindex.FileEntry(name, datapair, len(data))
            root_entry.items[name] = entry
