Installation
============

Supported Platforms
-------------------
Python 2.6, 2.7, 3.3, and 3.4 are supported.  Both CPython (the standard Python
implementation) and `PyPy <http://pypy.org>`_ are supported and tested.

Linux, OSX, and Windows are supported.

Installation through pip
------------------------
`pip <https://pypi.python.org/pypi/pip>`_ is the recommended tool for installing
packages.  It will handle installing all Python dependencies for the driver at
the same time as the driver itself.  To install the extension from pypi::

    pip install cassandra-driver-dse

This will also pull down the core driver from pypi. To avoid building Cython extensions
in the core driver, use the environment variable switch::

    CASS_DRIVER_NO_CYTHON=1 pip install cassandra-driver-dse

For more information on core driver optional dependencies, see the `installation guide <http://datastax.github.io/python-driver/installation.html>`_.

Verifying your Installation
---------------------------
To check if the installation was successful, you can run::

    python -c 'import dse; print dse.__version__'

It should print something like "1.0.0".
