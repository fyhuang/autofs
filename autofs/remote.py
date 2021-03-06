from __future__ import unicode_literals, print_function, absolute_import

import pickle

import autofs.protobuf.autofs_pb2 as pb2
from autofs import tuneables, userconfig, network

def read(inst, fentry, length, offset):
    conns = network.get_connections()
    if len(conns) == 0:
        print("No remote connections!")
        return None
    c = conns.values()[0]

    #if fentry.size < tuneables.PARTIAL_READ_FILESIZE:
    msg = pb2.GetBlocks()
    msg.block_ids.append(fentry.block_id)
    print("Reading {}".format(fentry.block_id))
    c.send(pb2.GET_BLOCKS, msg)
    result = c.get_result(pb2.BLOCKS_DATA)

    # Cache the result locally
    inst.fs.cache(fentry.block_id, result.data)

    return result.data[offset:offset+length]

def send_cluster_info(conn):
    msg = pb2.ClusterInfo()
    msg.cluster_id = conn.inst.static_info['cluster_id']
    for bid,b in conn.inst.fi.bundles.items():
        entry = msg.bundles.add()
        entry.bundle_id = bid
        entry.latest_version = b.latest().version

    index_blob = pickle.dumps(conn.inst.fi.minimal_copy(conn.inst.fs))
    conn.send(pb2.CLUSTER_INFO, msg, index_blob)

def send_peer_announce(conn):
    if not conn.peer_announce_sent:
        msg = pb2.PeerAnnounce()
        msg.peer_id = userconfig.get_user_config()['peerid']
        msg.version = "0.1"
        msg.proto_version = 1
        msg.cluster_id = conn.inst.static_info['cluster_id']
        conn.send(pb2.PEER_ANNOUNCE, msg)
        
        conn.peer_announce_sent = True

def handle_packet(conn, packet):
    header = packet.header

    if header[network.H_MTYPE] == pb2.PEER_ANNOUNCE:
        send_peer_announce(conn)

    elif header[network.H_MTYPE] == pb2.JOIN_CLUSTER:
        send_cluster_info(conn)
        send_peer_announce(conn)
    
    elif header[network.H_MTYPE] == pb2.GET_BLOCKS:
        get_blocks = packet.message

        resp = pb2.BlocksData()
        data = bytearray()
        for b in get_blocks.block_ids:
            resp.block_ids.append(b)
            with conn.inst.fs.blockfile(b) as f:
                data.extend(f.read())
        conn.send(pb2.BLOCKS_DATA, resp, data)

    return False
