import proto.autofs_pb2 as pb2

import tuneables

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

def handle_packet(conn, header, pbuf_blob, data_blob):
    if header[H_MTYPE] == pb2.JOIN_CLUSTER:
        msg = pb2.ClusterInfo()
        msg.cluster_id = self.inst.static_info['cluster_id']
        for bid,b in self.inst.fi.bundles.items():
            entry = msg.bundles.add()
            entry.bundle_id = bid
            entry.latest_version = b.latest().version
        self.send(pb2.CLUSTER_INFO, msg)

        msg = pb2.PeerAnnounce()
        msg.peer_id = userconfig.get_user_config()['peerid']
        msg.version = "0.1"
        msg.proto_version = 1
        self.send(pb2.PEER_ANNOUNCE, msg)
