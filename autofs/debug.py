from __future__ import unicode_literals, print_function, absolute_import

import autofs.protobuf.autofs_pb2 as pb2
import autofs.protobuf.autofs_local_pb2 as lpb2


def msg_type_str(mt):
    for v in pb2._MESSAGETYPE.values:
        if v.number == mt:
            return v.name
    return 'unknown'
def msg_type_str_local(mt):
    for v in lpb2._MESSAGETYPE.values:
        if v.number == mt:
            return v.name
    return 'unknown'


def err_code_str_local(ec):
    for v in lpb2._ERRORCODE.values:
        if v.number == ec:
            return v.name
    return 'unknown'


def header_str(header):
    return "{} (len {}, datalen {})".format(
        msg_type_str(header[0]),
        header[2],
        header[1]
        )


def print_packet(packet_tup):
    mtype = ord(pkt[0])
    mpkt = pkt[1:]
    if mtype == pb2.REQ_STAT:
        pb = pb2.ReqStat()
    elif mtype == pb2.REQ_LISTDIR:
        pb = pb2.ReqListdir()
    elif mtype == pb2.REQ_READ:
        pb = pb2.ReqRead()

    elif mtype == pb2.RESP_ERROR:
        pb = "error " + err_code_str(ord(mpkt))
    elif mtype == pb2.RESP_STAT:
        pb = pb2.RespStat()
    elif mtype == pb2.RESP_LISTDIR:
        pb = pb2.RespListdir()
    elif mtype == pb2.RESP_READ:
        pb = to_hex(mpkt)
    else:
        pb = "unknown"

    if not isinstance(pb, str):
        pb.ParseFromString(mpkt)

    print("{0}: {1}".format(msg_type_str(mtype), pb))
