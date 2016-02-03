# Copyright 2016 DataStax, Inc.

__version_info__ = (0, 0, 1, 'post0')
__version__ = '.'.join(map(str, __version_info__))

def _open_doc():
    import os
    import webbrowser
    index_path = 'file://%s/doc/index.html' % (os.path.dirname(os.path.realpath(__file__)),)
    webbrowser.open(index_path)
