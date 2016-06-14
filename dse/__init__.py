# Copyright 2016 DataStax, Inc.

import os

__version_info__ = (1, 0, '0a2', 'post0')
__version__ = '.'.join(map(str, __version_info__))

_core_driver_target_version = '3.5.1'
_use_any_core_driver_version = bool(os.environ.get('DSE_DRIVER_PERMIT_UNSUPPORTED_CORE'))

def _open_doc():
    import os
    import webbrowser
    index_path = 'file://%s/doc/index.html' % (os.path.dirname(os.path.realpath(__file__)),)
    webbrowser.open(index_path)
