import os.path
import sys
import signal

from gevent_zeromq import zmq
import gevent

from autofs import userconfig, local, instance, peer_responder
from autofs import cmd_bundle, cmd_join

def main():
    if len(sys.argv) < 2:
        print("""Usage:
\tautofs command instance-name

Commands:
\tserve
\tbundle""")
        sys.exit(1)

    command = sys.argv[1]
    instance_path = os.path.realpath(sys.argv[2])
    command_args = sys.argv[3:]

    gevent.signal(signal.SIGQUIT, gevent.shutdown)

    if command == 'init':
        if not os.path.isdir(instance_path):
            instance.Instance.create(instance_path)
        else:
            print("Instance already exists at {}".format(instance_path))

    elif command == 'serve':
        inst = instance.Instance.load(instance_path)

        print("0MQ version: " + zmq.zmq_version())
        ctx = zmq.Context()
        #ls = local.start_server(ctx, inst)
        rs = peer_responder.start_server(inst)
        #glets = [ls, rs]
        #gevent.joinall(glets)
        rs.join()

    elif command == 'connect':
        # Client?
        inst = instance.Instance.load(instance_path)
        ctx = zmq.Context()
        ls = local.start_server(ctx, inst)
        pf = peer_responder.find_peers(inst)
        glets = [ls, pf]
        gevent.joinall(glets)

    elif command == 'join':
        if len(command_args) == 0:
            print("Usage: autofs join instance-name remote-host")
        if os.path.isdir(instance_path):
            print("Cannot join using an existing instance")

        inst = cmd_join.join(instance_path, command_args[0])

    elif sys.argv[1] == 'bundle':
        if len(command_args) == 0:
            print("Usage: autofs bundle instance-name target-dir")
            sys.exit(1)

        if not os.path.isdir(instance_path):
            inst = instance.Instance.create(instance_path)
        else:
            inst = instance.Instance.load(instance_path)

        cmd_bundle.bundle(inst, command_args[0])

        inst.save()
        print("Done")

if __name__ == "__main__":
    main()
