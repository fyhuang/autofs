import unittest
import tempfile
import shutil

import gevent
import autofs.protobuf.autofs_pb2 as pb2
from autofs import instance, network, userconfig, remote

from autofs.cmd import cmd_join

class TestNetwork(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp('autofs')
        self.inst = instance.Instance.create(self.tempdir)
        self.server = network.start_server(self.inst)
        gevent.sleep(1)
        self.pc = network.connect(None, ('127.0.0.1', 1234))
        
        userconfig._uc = {'peerid': '254af13a-e698-11e1-a3d2-68a86d09cc32'}

    def test_joincluster(self):
        conn = self.pc[0]

        msg = pb2.JoinCluster()
        msg.peer_id = userconfig.get_user_config()['peerid']
        msg.version = "0.1"
        msg.proto_version = 1
        conn.send(pb2.JOIN_CLUSTER, msg)

        msg = conn.get_result(pb2.CLUSTER_INFO).message
        self.assertEqual(msg.cluster_id, self.inst.static_info['cluster_id'])

        msg = conn.get_result(pb2.PEER_ANNOUNCE).message
        self.assertEqual(msg.cluster_id, self.inst.static_info['cluster_id'])
        self.assertEqual(msg.peer_id, userconfig.get_user_config()['peerid'])
        self.assertEqual(msg.peer_id, conn.peer_id)

    def test_peerannounce(self):
        conn = self.pc[0]
        conn.inst = self.inst

        remote.send_peer_announce(conn)

        msg = conn.get_result(pb2.PEER_ANNOUNCE).message
        self.assertEqual(msg.cluster_id, self.inst.static_info['cluster_id'])
        self.assertEqual(msg.peer_id, userconfig.get_user_config()['peerid'])
        self.assertEqual(msg.peer_id, conn.peer_id)

    def tearDown(self):
        self.pc[0].close()
        gevent.sleep(0)
        self.server.kill()
        shutil.rmtree(self.tempdir)

if __name__ == "__main__":
    unittest.main()
