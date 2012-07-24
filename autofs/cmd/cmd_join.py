import gevent
import pickle

import autofs.proto.autofs_pb2 as pb2
from autofs import instance, peer_responder, userconfig

def join(instance_path, source_name):
    print("Connecting to {}:1234".format(source_name))
    # Get the peerid of the source
    conn, cg = peer_responder.connect(None, (source_name, 1234))
    msg = pb2.JoinCluster()
    msg.peer_id = userconfig.get_user_config()['peerid']
    msg.version = "0.1"
    msg.proto_version = 1
    conn.send(pb2.JOIN_CLUSTER, msg)

    cluster_info_pkt = conn.get_result(pb2.CLUSTER_INFO)
    msg = pb2.ClusterInfo()
    msg.ParseFromString(cluster_info_pkt[1])

    print("Joining cluster {}".format(msg.cluster_id))
    static_info = {'cluster_id': msg.cluster_id}
    inst = instance.Instance.create(instance_path, static_info)

    print("Unpickling file index")
    inst.fi = pickle.loads(cluster_info_pkt[2])

    peer_info_pkt = conn.get_result(pb2.PEER_ANNOUNCE)
    msg = pb2.PeerAnnounce()
    msg.ParseFromString(peer_info_pkt[1])

    conn.close()

    pi = inst.get_peer_info(msg.peer_id)
    pi.update_addr((source_name, 1234))
    inst.save()
