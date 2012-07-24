from autofs import fsindex

def list_items(inst):
    # TODO: filter list

    def list_recur(ix, prefix):
        for name, item in ix.items.items():
            print("{} {}".format(item.ftype, prefix + name))
            if item.ftype == fsindex.DIR:
                list_recur(item, prefix + name + '/')

    for bid, b in inst.fi.bundles.items():
        index = b.latest()
        list_recur(index.index, '/')
