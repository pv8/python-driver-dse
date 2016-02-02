Getting Started
===============

First, make sure you have the dse extension properly :doc:`installed <installation>`.

Connecting to Cassandra
-----------------------
The DSE extension builds on the core DataStax Cassandra Driver. Using the DSE
extension is as simple as importing ``Cluster`` from the ``dse`` package instead of the core
``cassadnra`` package:

.. code-block:: python

    from dse.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()
    session.execute("SELECT * FROM system.local")

These types extend the core API, but behave exactly like the core driver counterparts for
the purpose of CQL execution (`core documentation here <http://datastax.github.io/python-driver/index.html>`_).

For examples using the extension with specific DSE features, see the pertinent sections below:

- :doc:`DSE Authentication <auth>`
- :doc:`Graph queries <graph>`
- :doc:`Geometric types <geo_types>`
