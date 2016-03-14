# Copyright 2016 DataStax, Inc.

import json
import six

from cassandra.cluster import Cluster, Session
import dse.cqltypes  # unsued here, imported to cause type registration
from dse.graph import GraphOptions, SimpleGraphStatement, graph_object_row_factory
from dse.util import Point, LineString, Polygon


class Cluster(Cluster):
    """
    Cluster extending `cassandra.cluster.Cluster <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster>`_.
    The API is identical, except that it returns a :class:`dse.cluster.Session` (see below).
    """

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
        and returns a GraphResultSet from this execution.

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
        if isinstance(query, SimpleGraphStatement):
            options = query.options.get_options_map(self.default_graph_options)
        else:
            query = SimpleGraphStatement(query)
            options = self.default_graph_options._graph_options

        graph_parameters = None
        if parameters:
            graph_parameters = self._transform_params(parameters)

        # TODO:
        # this is basically Session.execute_async, repeated here to customize the row factory. May want to add that
        # parameter to the session method
        if timeout is _NOT_SET:
            timeout = self.default_graph_timeout
        future = self._create_response_future(query, parameters=None, trace=trace, custom_payload=options, timeout=timeout)
        future.message._query_params = graph_parameters
        future._protocol_handler = self.client_protocol_handler
        future.row_factory = row_factory or self.default_graph_row_factory
        future.send_request()
        return future.result()

    def _transform_params(self, parameters):
        if not isinstance(parameters, dict):
            raise ValueError('The parameters must be a dictionary. Unnamed parameters are not allowed.')
        return [json.dumps(parameters).encode('utf-8')]
