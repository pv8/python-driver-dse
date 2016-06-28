Getting Started
===============

First, make sure you have the DSE driver properly :doc:`installed <installation>`.

Upgrading Existing Code from Core Driver
----------------------------------------
Minimal Property Settings
~~~~~~~~~~~~~~~~~~~~~~~~~
Upgrading from ``cassandra-driver`` to ``python-driver-dse`` can be as simple as changing the Cluster import
to the ``dse`` package:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()
    print session.execute("SELECT release_version FROM system.local")[0]

...becomes:

.. code-block:: python

    from dse.cluster import Cluster

    cluster = Cluster()
    session = cluster.connect()
    print session.execute("SELECT release_version FROM system.local")[0]

Changes in Execution Property Defaults
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The DSE Driver takes advantage of *configuration profiles* to allow different execution configurations for the various
query handlers. Please see the `Execution Profile documentation <http://datastax.github.io/python-driver/execution_profiles.html>`_
for a more generalized discussion of the API. What follows here is an upgrade guide for DSE driver, which uses this API.
Using this API disallows use of the legacy config parameters, so changes must be made when setting non-default options
for any of these parameters:

- ``Cluster.load_balancing_policy``
- ``Cluster.retry_policy``
- ``Session.default_timeout``
- ``Session.default_consistency_level``
- ``Session.default_serial_consistency_level``
- ``Session.row_factory``

For example:

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.query import tuple_factory

    cluster = Cluster()
    session = cluster.connect()
    session.row_factory = tuple_factory

    print session.execute("SELECT release_version FROM system.local")[0]

Here we are only setting one of these attributes, so we use the default profile and change that one attribute:

.. code-block:: python

    from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.query import tuple_factory
    from dse.cluster import Cluster

    profile = ExecutionProfile(row_factory=tuple_factory)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect()

    print session.execute("SELECT release_version FROM system.local")[0]

Profiles are passed in by ``execution_profile`` dict.

Here we have more default execution parameters being set:

.. code-block:: python

    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster
    from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
    from cassandra.query import tuple_factory

    cluster = Cluster(load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
                      default_retry_policy=DowngradingConsistencyRetryPolicy())
    session = cluster.connect()
    session.default_timeout = 15
    session.row_factory = tuple_factory
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    session.default_serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL

    print session.execute("SELECT release_version FROM system.local")[0]

In this case we can construct the base ``ExecutionProfile`` passing all attributes:

.. code-block:: python

    from cassandra import ConsistencyLevel
    from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
    from cassandra.query import tuple_factory
    from dse.cluster import Cluster

    profile = ExecutionProfile(WhiteListRoundRobinPolicy(['127.0.0.1']),
                               DowngradingConsistencyRetryPolicy(),
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_SERIAL,
                               15, tuple_factory)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect()

    print session.execute("SELECT release_version FROM system.local")[0]

It should be noted that this sets the default behavior for CQL requests. The DSE driver also defines a set of default
profiles for graph execution:

* :data:`~.cluster.EXEC_PROFILE_GRAPH_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT`
* :data:`~.cluster.EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT`

Users are free to setup additional profiles to be used by name:

.. code-block:: python

    profile_long = ExecutionProfile(request_timeout=30)
    cluster = Cluster(execution_profiles={'long': profile_long})
    session = cluster.connect()
    session.execute(statement, execution_profile='long')

Also, parameters passed to ``Session.execute`` or attached to ``Statement``\s are still honored as before.

Connecting to DSE
-----------------
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
