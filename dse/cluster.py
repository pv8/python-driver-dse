# Copyright 2016 DataStax, Inc.
import json
import logging

from cassandra.cluster import Cluster, Session, default_lbp_factory, ExecutionProfile
from cassandra.query import tuple_factory
import dse.cqltypes  # unsued here, imported to cause type registration
from dse.graph import GraphOptions, SimpleGraphStatement, graph_object_row_factory
from dse.policies import HostTargetingPolicy, NeverRetryPolicy
from dse.query import HostTargetingStatement
from dse.util import Point, LineString, Polygont


log = logging.getLogger(__name__)

EXEC_PROFILE_GRAPH_DEFAULT = object()
EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT = object()


class GraphExecutionProfile(ExecutionProfile):

    graph_options = None
    """
    :class:`.GraphOptions` to use with this execution
    """

    def __init__(self, load_balancing_policy=None, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=10.0, row_factory=graph_object_row_factory,
                 graph_options=None):
        retry_policy = retry_policy or NeverRetryPolicy()
        super(GraphExecutionProfile, self).__init__(load_balancing_policy, retry_policy, consistency_level,
                                                    serial_consistency_level, request_timeout, row_factory)
        self.graph_options = graph_options or GraphOptions()


class GraphAnalyticsExecutionProfile(GraphExecutionProfile):

    def __init__(self, load_balancing_policy=None, retry_policy=None,
                 consistency_level=ConsistencyLevel.LOCAL_ONE, serial_consistency_level=None,
                 request_timeout=10.0, row_factory=graph_object_row_factory,
                 graph_options=None):
        # TODO: get new default timeouts
        load_balancing_policy = load_balancing_policy or HostTargetingPolicy(default_lbp_factory())
        super(GraphExecutionProfile, self).__init__(load_balancing_policy, retry_policy, consistency_level,
                                                    serial_consistency_level, request_timeout, row_factory, graph_options)


class Cluster(Cluster):
    """
    Cluster extending `cassandra.cluster.Cluster <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster>`_.
    The API is identical, except that it returns a :class:`dse.cluster.Session` (see below).
    The default load_balancing_policy adds master host targeting for graph analytics queries.
    """
    def __init__(self, *args, **kwargs):
        super(Cluster, self).__init__(*args, **kwargs)

        self.profile_manager.profiles.setdefault(EXEC_PROFILE_GRAPH_DEFAULT, GraphExecutionProfile())
        self.profile_manager.profiles.setdefault(EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT, GraphAnalyticsExecutionProfile())

    def _new_session(self):
        session = Session(self, self.metadata.all_hosts())
        self._session_register_user_types(session)
        self.sessions.add(session)
        return session


_NOT_SET = object()


class Session(Session):
    """
    A session extension based on `cassandra.cluster.Session <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session>`_
    with additional features:

        - Pre-registered DSE-specific types (geometric types)
        - Graph execution API
    """

    default_graph_options = None
    """
    Default options for graph queries, initialized as follows by default::

        GraphOptions(graph_source=b'default',
                     graph_language=b'gremlin-groovy')

    See dse.graph.GraphOptions
    """

    default_graph_row_factory = staticmethod(graph_object_row_factory)
    """
    Row factory used for graph results.
    The default is dse.graph.graph_result_row_factory.
    """

    default_graph_timeout = 32.0
    """
    A default timeout (seconds) for graph queries executed with
    :meth:`.execute_graph()`.  This default may be overridden with the
    `timeout` parameter of that method.

    Setting this to :const:`None` will cause no timeouts to be set by default.

    .. versionadded:: 1.0.0
    """

    def __init__(self, cluster, hosts):

        super(Session, self).__init__(cluster, hosts)

        def cql_encode_str_quoted(val):
            return "'%s'" % val

        for typ in (Point, LineString, Polygon):
            self.encoder.mapping[typ] = cql_encode_str_quoted

        self.default_graph_options = GraphOptions(graph_source=b'default',
                                                  graph_language=b'gremlin-groovy')

    def execute_graph(self, query, parameters=None, timeout=_NOT_SET, trace=False, row_factory=None):
        """
        Executes a Gremlin query string or SimpleGraphStatement synchronously,
        and returns a ResultSet from this execution.

        `parameters` is dict of named parameters to bind. The values must be
        JSON-serializable.
        (TBD: make this customizable)

        `timeout` and `trace` have the same meaning as in `Session.execute <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session.execute>`_.

        `row_factory` defines how the results of this query are returned. If not set,
        it defaults to :attr:`Session.default_graph_row_factory`.

        Example usage::

            >>> session = cluster.connect()
            >>> statement = GraphStatement('x.v()')
            >>> statement.options.graph_binding = 'x'  # non-standard option set
            >>> results = session.execute_graph(statement)
            >>> for result in results:
            ...     print(result.value)  # defaults results are dse.graph.Result
        """
        if not isinstance(query, SimpleGraphStatement):
            query = SimpleGraphStatement(query)
        options = self.default_graph_options.copy()
        options.update(query.options)

        graph_parameters = None
        if parameters:
            graph_parameters = self._transform_params(parameters)

        if timeout is _NOT_SET:
            timeout = self.default_graph_timeout
        future = self._create_response_future(query, parameters=None, trace=trace, custom_payload=options.get_options_map(), timeout=timeout)
        future.message._query_params = graph_parameters
        future._protocol_handler = self.client_protocol_handler
        future.row_factory = row_factory or self.default_graph_row_factory

        if options.is_analytics_source and isinstance(self._load_balancer, HostTargetingPolicy):
            self._target_analytics_master(future)
        else:
            future.send_request()
        return future.result()

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
