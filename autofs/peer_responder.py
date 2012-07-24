import struct

import gevent
from gevent import server, socket, coros, queue, event

import proto.autofs_pb2 as pb2
from autofs import userconfig, remote

HEADER_FMT = "<HHL"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

class PeerConnection(object):
    def __init__(self, inst, sock, addr):
        self.inst = inst
        self.sock = sock
        self.remote_addr = addr
        self.peer_id = None

        self.in_queue = queue.Queue()

        self.results_lock = coros.Semaphore()
        self.results_evt = event.Event()
        self.results = []

        self.greelets = None

    def handle(self):
        def sender():
            while True:
                mtrip = self.in_queue.get() # mtype, pbuf, (optional) data

                binary_len = 0
                if mtrip[2] is not None:
                    binary_len = len(mtrip[2])
                mpkt = mtrip[1].SerializeToString()

                self.sock.send(struct.pack(HEADER_FMT, mtrip[0], binary_len, len(mpkt) + binary_len))
                self.sock.send(mpkt)
                if mtrip[2] is not None:
                    self.sock.send(mtrip[2])
                print("Sent {}".format(mtrip[0]))

        def receiver():
            while True:
                header_blob = self.sock.recv(HEADER_SIZE, socket.MSG_WAITALL)
                if len(header_blob) == 0:
                    print("{} disconnected".format(self.remote_addr))
                    # TODO shutdown sender too
                    return
                try:
                    header = struct.unpack(HEADER_FMT, header_blob)
                except:
                    print(len(header))
                    raise
                print(header)
                if header[remote.H_BINARYLEN] == 0:
                    pbuf_blob = self.sock.recv(header[remote.H_LEN], socket.MSG_WAITALL)
                    data_blob = None
                else:
                    binary_len = header[remote.H_BINARYLEN]
                    pbuf_len = header[remote.H_LEN] - binary_len
                    pbuf_blob = self.sock.recv(pbuf_len, socket.MSG_WAITALL)
                    # TODO gevent.sleep(0)
                    data_blob = self.sock.recv(binary_len, socket.MSG_WAITALL)

                # Update peer info
                if header[remote.H_MTYPE] == pb2.PEER_ANNOUNCE:
                    msg = pb2.PeerAnnounce()
                    msg.ParseFromString(pbuf_blob)
                    self.peer_id = peer_id
                elif header[remote.H_MTYPE] == pb2.JOIN_CLUSTER:
                    msg = pb2.ClusterInfo()
                    msg.ParseFromString(pbuf_blob)
                    self.peer_id = peer_id

                if not remote.handle_packet(self, header, pbuf_blob, data_blob):
                    with self.results_lock:
                        self.results.append((header, pbuf_blob, data_blob))
                    self.results_evt.set()
                gevent.sleep(0)

        print("Connection from {}".format(self.remote_addr))
        self.greenlets = [gevent.spawn(sender), gevent.spawn(receiver)]
        return self.greenlets

    def send(self, mtype, msg, data=None):
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
    def _handle(sock, addr):
        conn = PeerConnection(inst, sock, addr)
        with connections_lock:
            connections[addr] = conn
        conn.handle()

    def _start():
        s = server.StreamServer(('0.0.0.0', 1234), _handle)
        s.serve_forever()

    return gevent.spawn(_start)

def connect(inst, addr):
    sock = socket.create_connection(addr)
    conn = PeerConnection(inst, sock, addr)
    with connections_lock:
        connections[addr] = conn

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
