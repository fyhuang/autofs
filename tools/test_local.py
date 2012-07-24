import zmq

import proto.autofs_local_pb2 as pb2
import autofs.debug as debug

ctx = zmq.Context()
sock = ctx.socket(zmq.REQ)
sock.connect("tcp://localhost:54321")

packets = []

pkt = pb2.ReqStat()
pkt.filepath = "/bndid/"
packets.append( (pb2.REQ_STAT, pkt) )

pkt = pb2.ReqStat()
pkt.filepath = "/bndid/test_file.txt"
packets.append( (pb2.REQ_STAT, pkt) )

pkt = pb2.ReqListdir()
pkt.dirpath = "/"
packets.append( (pb2.REQ_LISTDIR, pkt) )

pkt = pb2.ReqListdir()
pkt.dirpath = "/bndid"
packets.append( (pb2.REQ_LISTDIR, pkt) )

pkt = pb2.ReqListdir()
pkt.dirpath = "/bndid/dir"
packets.append( (pb2.REQ_LISTDIR, pkt) )

for p in packets:
    mpkt = p[1].SerializeToString()
    mtype = chr(p[0])

    msg = mtype + mpkt
    print("Sending:")
    debug.print_packet(msg)
    sock.send(msg)

    msg = sock.recv()
    print("Received:")
    debug.print_packet(msg)
