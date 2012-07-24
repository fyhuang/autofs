import stat
import time
from gevent_zeromq import zmq
import gevent

import fsindex
import debug

import proto.autofs_local_pb2 as pb2

def handle_packet(mtype, mpkt, fi):
    handler = None
    if mtype == pb2.REQ_STAT:
        pkt = pb2.ReqStat()
        pkt.ParseFromString(mpkt)
        handler = handle_stat
    elif mtype == pb2.REQ_LISTDIR:
        pkt = pb2.ReqListdir()
        pkt.ParseFromString(mpkt)
        handler = handle_listdir
    elif mtype == pb2.REQ_READ:
        pkt = pb2.ReqRead()
        pkt.ParseFromString(mpkt)
        handler = handle_read
    else:
        print("Unknown packet type {}!".format(mtype))
        return False

    msg = handler(pkt, fi)
    if isinstance(msg, int):
        return chr(pb2.RESP_ERROR) + chr(msg)
    return chr(msg[0]) + msg[1].SerializeToString()

def start_server(ctx, inst):
    def serve():
        sock = ctx.socket(zmq.REP)
        sock.bind("tcp://*:54321")
        while True:
            msg = sock.recv()
            debug.print_packet(msg)
            mtype = ord(msg[0])
            mpkt = msg[1:]

            msg = handle_packet(mtype, mpkt, inst)
            if msg != False:
                sock.send(msg)
            else:
                sock.send(chr(pb2.RESP_ERROR) + chr(pb2.ERR_UNKNOWN))
    return gevent.spawn(serve)

def dt_to_time(dt):
    return int(time.mktime(dt.timetuple()))

def stat_from_entry(entry):
    resp = pb2.RespStat()
    if entry.ftype == fsindex.FILE:
        resp.ftype = stat.S_IFREG
        resp.perms = 0644
        resp.inode = 2 # TODO entry.blobid
        resp.size = entry.size
    else:
        resp.ftype = stat.S_IFDIR
        resp.perms = 0755
        resp.inode = 1 # TODO
        resp.size = len(entry.items) + 2
    resp.ctime_utc = dt_to_time(entry.ctime)

    return pb2.RESP_STAT, resp

def do_stat(fi, bundle_id, filepath):
    if bundle_id not in fi.bundles.keys():
        return pb2.ERR_NOENT
    entry = fi.bundles[bundle_id].latest().traverse(filepath)
    if not entry:
        return pb2.ERR_NOENT
    return stat_from_entry(entry)

def handle_stat(pkt, inst):
    if pkt.filepath == '/':
        return pb2.ERR_UNKNOWN

    fp = pkt.filepath.strip('/').partition('/')
    bundle_id = fp[0]
    filepath = fp[1] + fp[2]
    return do_stat(inst.fi, bundle_id, filepath)

def handle_listdir(pkt, inst):
    fi = inst.fi
    if pkt.dirpath == '/':
        resp = pb2.RespListdir()
        for bundle_id, bundle in fi.bundles.iteritems():
            entry = resp.entry.add()
            entry.filename = bundle_id
            entry.stat.ftype = stat.S_IFDIR
            entry.stat.perms = 0755
            entry.stat.inode = 1
            entry.stat.size = len(bundle.latest().index.items)
            entry.stat.ctime_utc = 0
        return pb2.RESP_LISTDIR, resp

    dp = pkt.dirpath.strip('/').partition('/')
    bundle_id = dp[0]
    filepath = dp[1] + dp[2]
    if bundle_id not in fi.bundles.keys():
        return pb2.ERR_NOENT
    fentry = fi.bundles[bundle_id].latest().traverse(filepath)
    if fentry is None:
        return pb2.ERR_NOENT
    if fentry.ftype != fsindex.DIR:
        return pb2.ERR_INVALID_OP

    resp = pb2.RespListdir()
    for k,e in fentry.items.items():
        entry = resp.entry.add()
        entry.filename = k
        entry.stat.CopyFrom(stat_from_entry(e)[1])
    return pb2.RESP_LISTDIR, resp

def handle_read(pkt, inst):
    fi = inst.fi
    fp = pkt.filepath.strip('/').partition('/')
    bundle_id = fp[0]
    filepath = fp[1] + fp[2]
    if bundle_id not in fi.bundles.keys():
        return pb2.ERR_NOENT
    fentry = fi.bundles[bundle_id].latest().traverse(filepath)
    if fentry is None:
        return pb2.ERR_NOENT
    if fentry.ftype != fsindex.FILE:
        return pb2.ERR_INVALID_OP

    resp = pb2.RespRead()
    if inst.fs.has(fentry.datapair):
        resp.data = inst.fs.read(fentry.datapair, pkt.length, pkt.offset)
    else:
        resp.data = remote.read(fentry.datapair, pkt.length, pkt.offset)

    return pb2.RESP_READ, resp
