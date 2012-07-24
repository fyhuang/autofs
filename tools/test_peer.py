import struct
from gevent import socket
import proto.autofs_pb2 as pb2
import autofs.debug as debug

sock = socket.create_connection(('127.0.0.1', 1234))

HEADER_FMT = "<HHL"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

def send_packet(mtype, msg, data=None):
    binary_len = 0
    if data is not None:
        binary_len = len(data)
    mpkt = msg.SerializeToString()

    sock.send(struct.pack(HEADER_FMT, mtype, binary_len, len(mpkt) + binary_len))
    sock.send(mpkt)
    if data is not None:
        self.sock.send(data)
    
msg = pb2.JoinCluster()
msg.version = "0.1"
msg.proto_version = 1
send_packet(pb2.JOIN_CLUSTER, msg)

sock.close()
