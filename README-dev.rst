Most notes on releasing and testing are the same as those in the core driver `README-dev <https://github.com/datastax/python-driver/blob/master/README-dev.rst>`_.

Here we discuss any differences.

Releases
========
After the release has been tagged, add a section to docs.yaml with the new tag ref::

    versions:
      - name: <version name>
        ref: <release tag>

Running the Tests
=================
Test invocation is the same as in the core driver. However, these tests use some packages from the base test suite, so
they require those packages in the PYTHONPATH. To run these tests, first set the PYTHONPATH as follows::

    export PYTHONPATH=/path-to-core-repo/:/path-to-core-repo/tests

An example integration test run::

    DSE_DRIVER_PERMIT_UNSUPPORTED_CORE=1 ADS_HOME=/path-to-testeng-devtools/EmbeddedAds/ CASSANDRA_DIR=/path-to-built-dse DSE_VERSION=5.0.0 nosetests -s -v tests/integration/

Building the Docs
=================
Both drivers can still build raw Sphinx sites, using the setup script. We additionally provide
configuration for another tool called Documentor, which also uses the Sphinx source to build docs. 
This gives us versioned docs with nice integrated search.

Dependencies
------------
Sphinx
~~~~~~
Installed and built as described in core document.

Documentor
~~~~~~~~~~
To use documentor, clone and setup Documentor as specified in 
`the project <https://github.com/riptano/documentor#installation-and-quick-start>`_.
This tool assumes Ruby, bundler, and npm are present.

Building
--------
You will need the  documentor executable to be in the system path. You can either add it permanently or run with something
like this::

    PATH=$PATH:<documentor repo>/bin python setup.py doc

To build docs, we also need core driver in the PYTHONPATH. You can either ``pip install cassandra-driver``, or clone the repo
and put that in your path::

    git clone https://github.com/datastax/python-driver.git
    PYTHONPATH=$PYTHONPATH:./python-driver PATH=$PATH:<documentor repo>/bin python setup.py doc

The docs will not display properly just browsing the filesystem in a browser. To view the docs as they would be in most
web servers, use the SimpleHTTPServer module::

    cd docs/_build/
    python -m SimpleHTTPServer

Then, browse to `localhost:8000 <http://localhost:8000>`_.
