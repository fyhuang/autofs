from __future__ import unicode_literals, print_function, absolute_import

from autofs import fsindex

def list_items(inst):
    # TODO: filter list

    def list_recur(ix, prefix):
        for name, item in ix.items.items():
            print("{} {}".format(item.ftype, prefix + name))
            if item.ftype == fsindex.FILE:
                print(item.block_id)
            if item.ftype == fsindex.DIR:
                list_recur(item, prefix + name + '/')

    for bid, b in inst.fi.bundles.items():
        index = b.latest()
        print("Bundle: {} ({})".format(b.name, bid))
        list_recur(index.index, '/')
