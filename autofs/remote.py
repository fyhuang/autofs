import pickle

import proto.autofs_pb2 as pb2

from autofs import tuneables, userconfig

H_MTYPE = 0
H_BINARYLEN = 1
H_LEN = 2

def read(fentry, length, offset):
    conns = peer_responder.get_connections()
    if len(conns) == 0:
        return None
    c = conns[0]

    if fentry.size < tuneables.PARTIAL_READ_FILESIZE:
        msg = pb2.GetBlocks()
        msg.block_ids.append(fentry.datapair[1])
        c.send(pb2.GET_BLOCKS, msg)
        result = c.get_result(pb2.BLOCKS_DATA)
        return result[2]
    else:
        assert False
        return None

def send_cluster_info(conn):
    msg = pb2.ClusterInfo()
    msg.cluster_id = conn.inst.static_info['cluster_id']
    for bid,b in conn.inst.fi.bundles.items():
        entry = msg.bundles.add()
        entry.bundle_id = bid
        entry.latest_version = b.latest().version

    index_blob = pickle.dumps(conn.inst.fi.minimal_copy())
    conn.send(pb2.CLUSTER_INFO, msg, index_blob)

def handle_packet(conn, header, pbuf_blob, data_blob):
    if header[H_MTYPE] == pb2.JOIN_CLUSTER:
        send_cluster_info(conn)

        msg = pb2.PeerAnnounce()
        msg.peer_id = userconfig.get_user_config()['peerid']
        msg.version = "0.1"
        msg.proto_version = 1
        msg.cluster_id = conn.inst.static_info['cluster_id']
        conn.send(pb2.PEER_ANNOUNCE, msg)
