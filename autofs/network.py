import struct
import collections

import gevent
from gevent import server, socket, coros, queue, event

import autofs.protobuf.autofs_pb2 as pb2
from autofs import userconfig, debug

# TODO: need larger size for packets
HEADER_FMT = "<HLL"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

H_MTYPE = 0
H_BINARYLEN = 1
H_LEN = 2

mtype_to_pb2 = [
        None,
        pb2.JoinCluster,
        pb2.GetClusterInfo,
        pb2.BundleInfo,
        pb2.ClusterInfo,
        pb2.PeerAnnounce,
        pb2.GetBundleIndexes,
        pb2.GetBlocks,
        pb2.BlocksData,
        pb2.RegisterUpdateNotify,
        ]

packet_tuple = collections.namedtuple('packet_tuple', ['header', 'message', 'data'])
def get_packet_tuple(header, pbuf_bytes, data_bytes):
    mtype = header[H_MTYPE]
    if mtype > 0 and mtype < len(mtype_to_pb2):
        pbuf = mtype_to_pb2[mtype]()
        pbuf.ParseFromString(pbuf_bytes)
    else:
        pbuf = pbuf_bytes
        raise NotImplementedError()

    return packet_tuple(header, pbuf, data_bytes)

class PeerConnection(object):
    def __init__(self, inst, sock, addr, handler):
        """handler returns True to hide from results"""
        self.inst = inst
        self.sock = sock
        self.remote_addr = addr
        self.handler = handler

        # Peer info
        self.peer_id = None
        self.peer_announce_sent = False


        # Greenlets
        self.in_queue = queue.Queue()

        self.results_lock = coros.Semaphore()
        self.results_evt = event.Event()
        self.results = []

        self.greelets = None

    def recv_exact(self, count):
        data = bytearray()
        while len(data) < count:
            to_recv = min(8192, count-len(data))
            new_data = self.sock.recv(to_recv)
            if len(new_data) == 0:
                return b''
            print("Received {} bytes".format(len(new_data)))
            data.extend(new_data)
        return bytes(data)

    def handle(self):
        def sender():
            while True:
                mtrip = self.in_queue.get() # mtype, pbuf, (optional) data

                binary_len = 0
                if mtrip[2] is not None:
                    binary_len = len(mtrip[2])
                mpkt = mtrip[1].SerializeToString()
                packet_len = len(mpkt) + binary_len

                self.sock.sendall(struct.pack(HEADER_FMT, mtrip[0], binary_len, len(mpkt) + binary_len))
                self.sock.sendall(mpkt)
                if mtrip[2] is not None:
                    self.sock.sendall(mtrip[2])
                print("Outgoing: {}".format(debug.msg_type_str(mtrip[0])))

        def receiver():
            while True:
                header_blob = self.recv_exact(HEADER_SIZE)
                if len(header_blob) == 0:
                    print("{} disconnected".format(self.remote_addr))
                    # TODO shutdown sender too
                    return
                try:
                    header = struct.unpack(HEADER_FMT, header_blob)
                except:
                    print("Couldn't parse header: len {}".format(len(header)))
                    raise

                print("Incoming: {}".format(debug.header_str(header)))
                if header[H_BINARYLEN] == 0:
                    pbuf_blob = self.recv_exact(header[H_LEN])
                    data_blob = None
                else:
                    binary_len = header[H_BINARYLEN]
                    pbuf_len = header[H_LEN] - binary_len
                    print("pbuf_len: {}".format(pbuf_len))
                    pbuf_blob = self.recv_exact(pbuf_len)
                    print("received pbuf")
                    # TODO gevent.sleep(0)
                    data_blob = self.recv_exact(binary_len)
                print(len(pbuf_blob), type(data_blob))

                packet = get_packet_tuple(header, pbuf_blob, data_blob)

                # Update peer info
                if header[H_MTYPE] == pb2.PEER_ANNOUNCE or \
                        header[H_MTYPE] == pb2.JOIN_CLUSTER:
                    self.peer_id = packet.message.peer_id
                    print("Updated remote peer_id {}".format(self.peer_id))

                if self.handler is None or not self.handler(self, packet):
                    with self.results_lock:
                        self.results.append(packet)
                    self.results_evt.set()

                gevent.sleep(0)

        print("Connected to {}".format(self.remote_addr))
        self.greenlets = [gevent.spawn(sender), gevent.spawn(receiver)]
        return self.greenlets

    def send(self, mtype, msg, data=None):
        if __debug__:
            # Check for required fields
            s = msg.SerializeToString()
        self.in_queue.put((mtype, msg, data))
        gevent.sleep(0)

    def get_result(self, msg_type, nonblock=False):
        def _get_result():
            for i,r in enumerate(self.results):
                if r[0][0] == msg_type:
                    self.results.pop(i)
                    return r
            return None

        cnt = 0
        while True:
            with self.results_lock:
                res = _get_result()
            if res is not None:
                return res
            if nonblock and cnt > 0:
                return None
            # If result is not immediately available, try waiting for receiver
            self.results_evt.wait(timeout=1.0)
            self.results_evt.clear()
            cnt += 1

    def close(self):
        gevent.killall(self.greenlets)
        self.sock.close()
        print("Closed connection to {}".format(self.remote_addr))


connections_lock = coros.Semaphore()
connections = {}

def start_server(inst):
    assert inst is not None

    import remote

    def _handle(sock, addr):
        conn = PeerConnection(inst, sock, addr, remote.handle_packet)
        with connections_lock:
            connections[addr] = conn
        conn.handle()

    def _start():
        s = server.StreamServer(('0.0.0.0', 1234), _handle)
        s.serve_forever()

    return gevent.spawn(_start)

def connect(inst, addr):
    sock = socket.create_connection(addr)
    conn = PeerConnection(inst, sock, addr, None)
    with connections_lock:
        connections[addr] = conn

    if inst is not None:
        remote.send_peer_announce(conn)
    return conn, conn.handle()

def find_peers(inst):
    def _finder():
        for pid,p in inst.peer_info.items():
            gevent.spawn(connect, inst, p.last_seen_addr[-1])
        # TODO: use mDNS, central reporting service

    return gevent.spawn(_finder)

def get_connections():
    with connections_lock:
        return {conn.peer_id: conn for conn in connections.values() if conn.peer_id is not None}
