from __future__ import unicode_literals, print_function, absolute_import

import os
import os.path
import gevent
import pickle

import autofs.protobuf.autofs_pb2 as pb2
from autofs import instance, network, userconfig

def join_cluster(instance_path, source_name):
    print("Connecting to {}:1234".format(source_name))
    # Get the peerid of the source
    conn, cg = network.connect(None, (source_name, 1234))
    msg = pb2.JoinCluster()
    msg.peer_id = userconfig.get_user_config()['peerid']
    msg.version = "0.1"
    msg.proto_version = 1
    conn.send(pb2.JOIN_CLUSTER, msg)

    cluster_info_pkt = conn.get_result(pb2.CLUSTER_INFO)
    msg = cluster_info_pkt.message

    print("Joining cluster {}".format(msg.cluster_id))
    static_info = {'cluster_id': msg.cluster_id}
    if not os.path.isdir(instance_path):
        os.makedirs(instance_path)
    inst = instance.Instance.create(instance_path, static_info)

    print("Unpickling file index")
    inst.fi = pickle.loads(cluster_info_pkt.data)

    peer_info_pkt = conn.get_result(pb2.PEER_ANNOUNCE)
    msg = peer_info_pkt.message

    conn.close()

    pi = inst.get_peer_info(msg.peer_id)
    pi.update_addr((source_name, 1234))
    inst.save()

def join(instance_path, source_name):
    return join_cluster(instance_path, source_name)
