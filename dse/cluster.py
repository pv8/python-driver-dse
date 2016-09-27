# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
import json
import logging
import six

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import RemoteConnection
from gremlin_python.process.graph_traversal import GraphTraversal

from cassandra import ConsistencyLevel, __version__ as core_driver_version
from cassandra.cluster import Cluster, Session, default_lbp_factory, ExecutionProfile, _ConfigMode, _NOT_SET
from cassandra.marshal import int64_pack
from cassandra.query import tuple_factory
from dse import _core_driver_target_version, _use_any_core_driver_version, __version__ as dse_driver_version
import dse.cqltypes  # unsued here, imported to cause type registration
from dse.graph import GraphOptions, SimpleGraphStatement, graph_object_row_factory, graph_traversal_row_factory, _request_timeout_key
from dse.policies import DSELoadBalancingPolicy, NeverRetryPolicy
from dse.query import HostTargetingStatement
from dse.util import Point, LineString, Polygon
from dse.graphson import GraphSONWriter

if six.PY3:
    long = int

log = logging.getLogger(__name__)

EXEC_PROFILE_GRAPH_DEFAULT = object()
"""
Key for the default graph execution profile, used when no other profile is selected in
``Session.execute_graph(execution_profile)``.

Use this as the key in `Cluster(execution_profiles) <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster>`_
to override the default graph profile.
"""

EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT = object()
"""
Key for the default graph system execution profile. This can be used for graph statements using the DSE graph
system API.

Selected using ``Session.execute_graph(execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)``.
"""

EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT = object()
"""
Key for the default graph analytics execution profile. This can be used for graph statements intended to
use Spark/analytics as the traversal source.

Selected using ``Session.execute_graph(execution_profile=EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT)``.
"""

EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT = object()
"""
Key for the default graph traversal execution profile, used when no other profile is selected in
``Session.execute_traversal(execution_profile)``.

Use this as the key in `Cluster(execution_profiles) <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster>`_
to override the default graph profile.
"""


class GraphExecutionProfile(ExecutionProfile):
    graph_options = None
    """
    :class:`.GraphOptions` to use with this execution

    Default options for graph queries, initialized as follows by default::

        GraphOptions(graph_source='g',
                     graph_language='gremlin-groovy')

    See dse.graph.GraphOptions
    """

    def __init__(self, load_balancing_policy=None, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=30.0, row_factory=graph_object_row_factory,
                 graph_options=None):
        """
        Default execution profile for graph execution.

        See `ExecutionProfile <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.ExecutionProfile>`_
        for base attributes.

        In addition to default parameters shown in the signature, this profile also defaults ``retry_policy`` to
        :class:`dse.policies.NeverRetryPolicy`.
        """
        retry_policy = retry_policy or NeverRetryPolicy()
        super(GraphExecutionProfile, self).__init__(load_balancing_policy, retry_policy, consistency_level,
                                                    serial_consistency_level, request_timeout, row_factory)
        self.graph_options = graph_options or GraphOptions(graph_source='g',
                                                           graph_language='gremlin-groovy')


class GraphAnalyticsExecutionProfile(GraphExecutionProfile):

    def __init__(self, load_balancing_policy=None, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=3600. * 24. * 7., row_factory=graph_object_row_factory,
                 graph_options=None):
        """
        Execution profile with timeout and load balancing appropriate for graph analytics queries.

        See also :class:`~.GraphExecutionPolicy`.

        In addition to default parameters shown in the signature, this profile also defaults ``retry_policy`` to
        :class:`dse.policies.NeverRetryPolicy`, and ``load_balancing_policy`` to one that targets the current Spark
        master.
        """
        load_balancing_policy = load_balancing_policy or DSELoadBalancingPolicy(default_lbp_factory())
        graph_options = graph_options or GraphOptions(graph_source='a',
                                                      graph_language='gremlin-groovy')
        super(GraphAnalyticsExecutionProfile, self).__init__(load_balancing_policy, retry_policy, consistency_level,
                                                             serial_consistency_level, request_timeout, row_factory, graph_options)


class GraphTraversalExecutionProfile(GraphExecutionProfile):
    graph_options = None
    """
    :class:`.GraphOptions` to use with this execution

    Default options for graph traversal queries, initialized as follows by default::

        GraphOptions(graph_source='g',
                     graph_language='bytecode-json')

    See dse.graph.GraphOptions
    """

    def __init__(self, load_balancing_policy=None, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=30.0, row_factory=graph_traversal_row_factory,
                 graph_options=None):
        """
        Default execution profile for graph traversal execution.

        See also :class:`~.GraphExecutionPolicy`.
        """
        super(GraphTraversalExecutionProfile, self).__init__(load_balancing_policy, retry_policy, consistency_level,
                                                            serial_consistency_level, request_timeout, row_factory)
        self.graph_options = graph_options or GraphOptions(graph_source='g',
                                                           graph_language='bytecode-json')


class Cluster(Cluster):
    """
    Cluster extending `cassandra.cluster.Cluster <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster>`_.
    The API is identical, except that it returns a :class:`dse.cluster.Session` (see below).

    It also uses the new `Execution Profile <http://datastax.github.io/python-driver/execution_profiles.html>`_ API, so
    legacy parameters are disallowed.
    """
    def __init__(self, *args, **kwargs):
        self._validate_core_version()

        super(Cluster, self).__init__(*args, **kwargs)

        if self._config_mode == _ConfigMode.LEGACY:
            raise ValueError("DSE Cluster uses execution profiles and should not specify legacy parameters "
                             "load_balancing_policy or default_retry_policy. Configure this in a profile instead.")

        lbp = DSELoadBalancingPolicy(default_lbp_factory())
        self.profile_manager.profiles.setdefault(EXEC_PROFILE_GRAPH_DEFAULT, GraphExecutionProfile(load_balancing_policy=lbp))
        self.profile_manager.profiles.setdefault(EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT, GraphExecutionProfile(load_balancing_policy=lbp, request_timeout=60. * 3.))
        self.profile_manager.profiles.setdefault(EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT, GraphAnalyticsExecutionProfile(load_balancing_policy=lbp))
        self.profile_manager.profiles.setdefault(EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT, GraphTraversalExecutionProfile(load_balancing_policy=lbp, request_timeout=60. * 3.))
        self._config_mode = _ConfigMode.PROFILES

    def _new_session(self, keyspace):
        session = Session(self, self.metadata.all_hosts(), keyspace)
        self._session_register_user_types(session)
        self.sessions.add(session)
        return session

    def _validate_core_version(self):
        if _core_driver_target_version != core_driver_version:
            if _use_any_core_driver_version:
                log.warning("DSE driver version %s is intended for use with core driver version %s. Environment overriden to use %s",
                            dse_driver_version, _core_driver_target_version, core_driver_version)
            else:
                import cassandra
                raise RuntimeError("DSE driver version %s is intended for use with core driver version %s. This environment is loading version %s from %s" % (dse_driver_version, _core_driver_target_version, core_driver_version, cassandra.__file__))


class _DSESessionRemoteGraphConnection(RemoteConnection):
    def __init__(self):
        super(_DSESessionRemoteGraphConnection, self).__init__(None, None)

    def submit(self, bytecode):
        raise RuntimeError("Cannot execute a GraphTraversal implicitly. Use session.execute_traversal().")


class Session(Session):
    """
    A session extension based on `cassandra.cluster.Session <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session>`_
    with additional features:

        - Pre-registered DSE-specific types (geometric types)
        - Graph execution API
    """

    def __init__(self, cluster, hosts, keyspace):

        super(Session, self).__init__(cluster, hosts, keyspace)

        def cql_encode_str_quoted(val):
            return "'%s'" % val

        for typ in (Point, LineString, Polygon):
            self.encoder.mapping[typ] = cql_encode_str_quoted

    def execute_graph(self, query, parameters=None, trace=False, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        """
        Executes a Gremlin query string or SimpleGraphStatement synchronously,
        and returns a ResultSet from this execution.

        `parameters` is dict of named parameters to bind. The values must be
        JSON-serializable.

        `execution_profile`: Selects an execution profile for the request.
        """
        return self.execute_graph_async(query, parameters, trace, execution_profile).result()

    def execute_graph_async(self, query, parameters=None, trace=False, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        """
        Execute the graph query and return a `ResponseFuture <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.ResponseFuture.result>`_
        object which callbacks may be attached to for asynchronous response delivery. You may also call ``ResponseFuture.result()`` to synchronously block for
        results at any time.
        """
        if not isinstance(query, SimpleGraphStatement):
            query = SimpleGraphStatement(query)

        graph_parameters = None
        if parameters:
            graph_parameters = self._transform_params(parameters)

        execution_profile = self._get_execution_profile(execution_profile)  # look up instance here so we can apply the extended attributes

        try:
            options = execution_profile.graph_options.copy()
        except AttributeError:
            raise ValueError("Execution profile for graph queries must derive from GraphExecutionProfile, and provide graph_options")

        custom_payload = options.get_options_map()
        custom_payload[_request_timeout_key] = int64_pack(long(execution_profile.request_timeout * 1000))
        future = self._create_response_future(query, parameters=None, trace=trace, custom_payload=custom_payload,
                                              timeout=_NOT_SET, execution_profile=execution_profile)
        future.message._query_params = graph_parameters
        future._protocol_handler = self.client_protocol_handler

        if options.is_analytics_source and isinstance(execution_profile.load_balancing_policy, DSELoadBalancingPolicy):
            self._target_analytics_master(future)
        else:
            future.send_request()
        return future

    def execute_traversal(self, traversal, trace=False, execution_profile=EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT):
        """
        Executes a Gremlin GraphTraversal synchronously, and returns a ResultSet from this execution.

        `execution_profile`: Selects an execution profile for the request.
        """
        return self.execute_traversal_async(traversal, trace=trace, execution_profile=execution_profile).result()

    def execute_traversal_async(self, traversal, trace=False, execution_profile=EXEC_PROFILE_GRAPH_TRAVERSAL_DEFAULT):
        """
        Execute a Gremlin GraphTraversal and return a `ResponseFuture <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.ResponseFuture.result>`_
        object which callbacks may be attached to for asynchronous response delivery. You may also call ``ResponseFuture.result()`` to synchronously block for
        results at any time.
        """

        if not isinstance(traversal, GraphTraversal):
            raise TypeError('traversal must be an instance of GraphTraversal.')

        try:
            query = GraphSONWriter.writeObject(traversal)
        except Exception:
            raise ValueError('Error while converting GraphTraversal to Graphson.')

        return self.execute_graph_async(query, trace=trace, execution_profile=execution_profile)

    def graph_traversal(self):
        """
        Returns a GraphTraversalSource for building GraphTraversal.

        See :attr:`dse.cluster.Cluster.execute_traversal` to execute a GraphTraversal.
        """
        graph = Graph()
        return graph.traversal().withRemote(_DSESessionRemoteGraphConnection())

    def _transform_params(self, parameters):
        if not isinstance(parameters, dict):
            raise ValueError('The parameters must be a dictionary. Unnamed parameters are not allowed.')
        return [json.dumps(parameters).encode('utf-8')]

    def _target_analytics_master(self, future):
        future._start_timer()
        master_query_future = self._create_response_future("CALL DseClientTool.getAnalyticsGraphServer()",
                                                           parameters=None, trace=False,
                                                           custom_payload=None, timeout=future.timeout)
        master_query_future.row_factory = tuple_factory
        master_query_future.send_request()

        cb = self._on_analytics_master_result
        args = (master_query_future, future)
        master_query_future.add_callbacks(callback=cb, callback_args=args, errback=cb, errback_args=args)

    def _on_analytics_master_result(self, response, master_future, query_future):
        try:
            row = master_future.result()[0]
            addr = row[0]['location']
            delimiter_index = addr.rfind(':')  # assumes <ip>:<port> - not robust, but that's what is being provided
            if delimiter_index > 0:
                addr = addr[:delimiter_index]
            targeted_query = HostTargetingStatement(query_future.query, addr)
            query_future.query_plan = self._load_balancer.make_query_plan(self.keyspace, targeted_query)
        except Exception:
            log.debug("Failed querying analytics master (request might not be routed optimally). "
                      "Make sure the session is connecting to a graph analytics datacenter.", exc_info=True)

        self.submit(query_future.send_request)
