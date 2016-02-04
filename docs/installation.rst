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
the same time as the driver itself.  To install the extension from downloaded tarball or zip::

    pip install <source-dist>

... where `<source-dist>` is the distribution downloaded from the EAP forum.

This will also pull down the pre-release core driver from pypi. To avoid building Cython extensions
in the core driver, use the environment variable switch::

    CASS_DRIVER_NO_CYTHON=1 pip install <source dist>

For more information on core driver optional dependencies, see the `installation guide <http://datastax.github.io/python-driver/installation.html>`_.

Verifying your Installation
---------------------------
To check if the installation was successful, you can run::

    python -c 'import dse; print dse.__version__'

It should print something like "1.0.0a1".

Documentation
-------------
This distribution comes with documentation embedded. It can be viewed in a browser on a desktop system by running::

    python -c 'import dse; dse._open_doc()'
