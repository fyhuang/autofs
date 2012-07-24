import os
import os.path
import sys
import uuid

_uc = None

def get_user_config():
    global _uc
    if _uc is None:
        if sys.platform == 'win32':
            assert False # TODO use registry on Windows
            return None

        config_dir = os.path.expanduser("~/.config/autofs/")
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
            with open(os.path.join(config_dir, 'peerid'), 'w') as f:
                f.write(uuid.uuid1().hex)

        with open(os.path.join(config_dir, 'peerid',), 'r') as f:
            peerid = f.read()

        _uc = {'peerid': peerid}
    return _uc
