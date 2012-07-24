import proto.autofs_local_pb2 as pb2


def msg_type_str(mt):
    for v in pb2._MESSAGETYPE.values:
        if v.number == mt:
            return v.name
    return 'unknown'


def err_code_str(ec):
    for v in pb2._ERRORCODE.values:
        if v.number == ec:
            return v.name
    return 'unknown'

to_hex = lambda x: "".join([hex(ord(c))[2:].zfill(2) for c in x])


def print_packet(pkt):
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
