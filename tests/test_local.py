from __future__ import unicode_literals, print_function, absolute_import

import unittest
import shutil
import stat

import gevent
from gevent import socket

import util
from autofs import local
import autofs.protobuf.autofs_local_pb2 as pb2

class TestLocal(unittest.TestCase):
    def setUp(self):
        self.tempdir, self.inst = util.create_test_instance()
        self.bundle = self.inst.fi.bundles.values()[0]

        self.local_server = local.start_server(self.inst)
        gevent.sleep(0)
        self.sock = socket.create_connection(('127.0.0.1', 54321))

    
    def test_readfiles(self):
        bid = self.bundle.bundle_id
        # Stat
        msg = pb2.ReqStat()
        msg.filepath = '/{}/test1'.format(bid)
        local.send_packet(self.sock, (pb2.REQ_STAT, msg, None))
        gevent.sleep(0)

        mtype, pbuf_bytes, data_bytes = local.recv_packet(self.sock)
        self.assertEqual(mtype, pb2.RESP_STAT)

        msg = pb2.RespStat()
        msg.ParseFromString(pbuf_bytes)
        self.assertEqual(msg.ftype, stat.S_IFREG)
        self.assertEqual(msg.perms, 0644)
        self.assertEqual(msg.size, self.bundle.latest().lookup('/test1').size)

        # Read
        msg = pb2.ReqRead()
        msg.filepath = '/{}/test1'.format(bid)
        msg.offset = 1
        msg.length = 4
        local.send_packet(self.sock, (pb2.REQ_READ, msg, None))
        gevent.sleep(0)

        mtype, pbuf_bytes, data_bytes = local.recv_packet(self.sock)
        self.assertEqual(mtype, pb2.RESP_READ)
        self.assertEqual(len(pbuf_bytes), 0)
        self.assertEqual(data_bytes, b'ello')
    
    def test_listdir(self):
        bid = self.bundle.bundle_id
        # dir_a
        msg = pb2.ReqListdir()
        msg.dirpath = '/{}/dir_a'.format(bid)
        local.send_packet(self.sock, (pb2.REQ_LISTDIR, msg, None))
        gevent.sleep(0)

        mtype, pbuf_bytes, data_bytes = local.recv_packet(self.sock)
        self.assertEqual(mtype, pb2.RESP_LISTDIR)

        msg = pb2.RespListdir()
        msg.ParseFromString(pbuf_bytes)
        self.assertEqual(len(msg.entries), 3)
        def entries_has_filename(fn):
            for e in msg.entries:
                if e.filename == fn:
                    return True
            return False
        self.assertTrue(entries_has_filename('test_file'))
        self.assertTrue(entries_has_filename('dir_b'))
        self.assertTrue(entries_has_filename('dir_c'))

        # dir_b
        msg = pb2.ReqListdir()
        msg.dirpath = '/{}/dir_a/dir_b'.format(bid)
        local.send_packet(self.sock, (pb2.REQ_LISTDIR, msg, None))
        gevent.sleep(0)

        mtype, pbuf_bytes, data_bytes = local.recv_packet(self.sock)
        self.assertEqual(mtype, pb2.RESP_LISTDIR)

        msg = pb2.RespListdir()
        msg.ParseFromString(pbuf_bytes)
        self.assertEqual(len(msg.entries), 1)
        self.assertTrue(msg.entries[0].filename == 'test_file')


    def tearDown(self):
        self.sock.close()
        gevent.sleep(0)
        self.local_server.kill()
        shutil.rmtree(self.tempdir)


if __name__ == "__main__":
    unittest.main()
