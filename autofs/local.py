import stat
import time
import struct

import gevent
from gevent import server

from autofs import fsindex, debug, remote, tempindex

import autofs.protobuf.autofs_local_pb2 as pb2

HEADER_FMT = "=HLL"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# Basic packet operations
def recv_exact(sock, count):
    data = bytearray()
    while len(data) < count:
        to_recv = min(8192, count-len(data))
        new_data = sock.recv(to_recv)
        if len(new_data) == 0:
            return b''
        data.extend(new_data)
        print("Received {} bytes ({}/{} total)".format(len(new_data), len(data), count))
    return bytes(data)

def recv_packet(sock):
    header_bytes = recv_exact(sock, HEADER_SIZE)
    if len(header_bytes) == 0:
        return None
    header = struct.unpack(HEADER_FMT, header_bytes)

    print("{} (pbuflen {}, datalen {})".format(
        debug.msg_type_str_local(header[0]),
        header[1],
        header[2]))

    pbuf_bytes = recv_exact(sock, header[1])
    data_bytes = recv_exact(sock, header[2])

    return header[0], pbuf_bytes, data_bytes

def send_packet(sock, mtrip):
    if mtrip[1] is not None:
        pbuf_bytes = mtrip[1].SerializeToString()
    else:
        pbuf_bytes = b''

    data_len = 0
    if mtrip[2] is not None:
        data_len = len(mtrip[2])
    
    sock.sendall(struct.pack(HEADER_FMT, mtrip[0], len(pbuf_bytes), data_len))
    sock.sendall(pbuf_bytes)
    if mtrip[2] is not None:
        sock.sendall(mtrip[2])


# Local server
def start_server(inst):
    def _handle(sock, addr):
        print("Connected to {}".format(addr))
        while True:
            # get request
            req = recv_packet(sock)
            if req is None:
                print("{} disconnected".format(addr))
                return
            mtype, pbuf_bytes, data_bytes = req

            # mtype, pbuf, data
            resp_trip = handle_packet(inst, mtype, pbuf_bytes, data_bytes)

            # send response
            send_packet(sock, resp_trip)

    def _start():
        s = server.StreamServer(('127.0.0.1', 54321), _handle)
        print("Starting local server")
        s.serve_forever()

    return gevent.spawn(_start)



# Protocol handlers
def handle_packet(inst, mtype, pbuf_bytes, data_bytes):
    if mtype not in mtype_to_types:
        print("Unknown packet type {}!".format(debug.msg_type_str_local(mtype)))
        return chr(pb2.RESP_ERROR) + chr(pb2.ERR_UNKNOWN)

    pbuf_type, handler = mtype_to_types[mtype]
    pkt = pbuf_type()
    pkt.ParseFromString(pbuf_bytes)

    msg = handler(pkt, inst, data_bytes)
    if isinstance(msg, int):
        return pb2.RESP_ERROR, None, struct.pack('B', msg)
    return msg


def dt_to_time(dt):
    return int(time.mktime(dt.timetuple()))

def parse_filepath(raw_filepath):
    dp = raw_filepath.strip('/').partition('/')
    bundle_id = dp[0]
    filepath = dp[1] + dp[2]
    return bundle_id, filepath

def get_entry(inst, raw_filepath):
    bundle_id, filepath = parse_filepath(raw_filepath)
    if bundle_id not in inst.fi.bundles.keys():
        return None
    bundle = inst.fi.bundles[bundle_id]
    if bundle.inflight is not None:
        return bundle.inflight.lookup(filepath)
    return bundle.latest().lookup(filepath)

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
    resp.mtime_utc = dt_to_time(entry.mtime)

    return pb2.RESP_STAT, resp, None


def handle_stat(pkt, inst, data_bytes):
    if pkt.filepath == '/':
        return pb2.ERR_UNKNOWN

    entry = get_entry(inst, pkt.filepath)
    if entry is None:
        return pb2.ERR_NOENT

    return stat_from_entry(entry)

def handle_listdir(pkt, inst, data_bytes):
    fi = inst.fi
    if pkt.dirpath == '/':
        resp = pb2.RespListdir()
        for bundle_id, bundle in fi.bundles.iteritems():
            entry = resp.entries.add()
            entry.filename = bundle_id
            entry.stat.ftype = stat.S_IFDIR
            entry.stat.perms = 0755
            entry.stat.inode = 1
            entry.stat.size = len(bundle.latest().index.items)
            entry.stat.mtime_utc = 0
        return pb2.RESP_LISTDIR, resp, None

    fentry = get_entry(inst, pkt.dirpath)
    if fentry.ftype != fsindex.DIR:
        return pb2.ERR_INVALID_OP

    resp = pb2.RespListdir()
    for k,e in fentry.items.items():
        entry = resp.entries.add()
        entry.filename = k
        entry.stat.CopyFrom(stat_from_entry(e)[1])
    return pb2.RESP_LISTDIR, resp, None


def handle_read(pkt, inst, data_bytes):
    fi = inst.fi

    fentry = get_entry(inst, pkt.filepath)
    if fentry is None:
        return pb2.ERR_NOENT
    if fentry.ftype != fsindex.FILE:
        return pb2.ERR_INVALID_OP

    if fentry.istemp:
        with open(fentry.datapath, 'rb') as f:
            f.seek(pkt.offset)
            resp = f.read(pkt.length)
    else:
        if fentry.block_id in inst.fs:
            with inst.fs.blockfile(fentry.block_id) as f:
                f.seek(pkt.offset)
                resp = f.read(pkt.length)
        else:
            resp = remote.read(fentry, pkt.length, pkt.offset)
            if resp is None:
                return pb2.ERR_UNKNOWN

    return pb2.RESP_READ, None, resp



def start_inflight(inst, bundle):
    if bundle.inflight is None:
        ti = tempindex.TempIndex(inst.path_to('tmp_{}'.format(bundle.bundle_id)), bundle.latest())
        bundle.inflight = ti


def handle_write(pkt, inst, data_bytes):
    bundle_id, filepath = parse_filepath(pkt.filepath)
    if bundle_id not in inst.fi.bundles:
        return pb2.ERR_NOENT
    bundle = inst.fi.bundles[bundle_id]

    start_inflight(inst, bundle)
    fentry = bundle.inflight.modify(filepath, inst.fs)

    with open(fentry.datapath, 'ab+') as out_f:
        out_f.seek(pkt.offset)
        out_f.write(data_bytes)

    return pb2.RESP_WRITE, None, None

def handle_truncate(pkt, inst, data_bytes):
    bundle_id, filepath = parse_filepath(pkt.filepath)
    if bundle_id not in inst.fi.bundles:
        return pb2.ERR_NOENT
    bundle = inst.fi.bundles[bundle_id]

    start_inflight(inst, bundle)
    fentry = bundle.inflight.modify(filepath, inst.fs)

    with open(fentry.datapath, 'ab+') as out_f:
        out_f.truncate(pkt.newLength)

    return pb2.RESP_WRITE, None, None

def handle_mknod(pkt, inst, data_bytes):
    bundle_id, filepath = parse_filepath(pkt.filepath)
    if bundle_id not in inst.fi.bundles:
        return pb2.ERR_NOENT
    bundle = inst.fi.bundles[bundle_id]

    start_inflight(inst, bundle)
    fentry = bundle.inflight.create(filepath, fsindex.FILE)

    return pb2.RESP_WRITE, None, None


mtype_to_types = {
        pb2.REQ_STAT: (pb2.ReqStat, handle_stat),
        pb2.REQ_LISTDIR: (pb2.ReqListdir, handle_listdir),
        pb2.REQ_READ: (pb2.ReqRead, handle_read),

        pb2.REQ_WRITE: (pb2.ReqWrite, handle_write),
        pb2.REQ_TRUNCATE: (pb2.ReqTruncate, handle_truncate),
        pb2.REQ_MKNOD: (pb2.ReqMknod, handle_mknod),
        }

