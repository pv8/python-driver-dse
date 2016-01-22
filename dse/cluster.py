import json
import six

from cassandra.cluster import Cluster, Session
import dse.cqltypes  # unsued here, imported to cause type registration
from dse.graph import GraphOptions, SimpleGraphStatement, graph_result_row_factory
from dse.util import Point, Circle, LineString, Polygon


class Cluster(Cluster):

    def _new_session(self):
        session = Session(self, self.metadata.all_hosts())
        self._session_register_user_types(session)
        self.sessions.add(session)
        return session


_NOT_SET = object()


class Session(Session):
    """
    A session extension based on cassandra.cluster.Session with additional features:

        - Pre-registered DSE-specific types (geometric types)
        - Graph execution API
    """

    default_graph_options = None
    """
    Default options, initialized as follows by default:
    GraphOptions(graph_source=b'default',
                 graph_language=b'gremlin-groovy')
    """

    default_graph_row_factory = staticmethod(graph_result_row_factory)


    def __init__(self, cluster, hosts):
        super(Session, self).__init__(cluster, hosts)

        def cql_encode_str_quoted(val):
            return "'%s'" % val

        for typ in (Point, Circle, LineString, Polygon):
            self.encoder.mapping[typ] = cql_encode_str_quoted

        self.default_graph_options = GraphOptions(graph_source=b'default',
                                                  graph_language=b'gremlin-groovy')

    def execute_graph(self, query, parameters=None, timeout=_NOT_SET, trace=False, row_factory=None):
        """
        Executes a Gremlin query string, a SimpleGraphStatement synchronously,
        and returns a GraphResultSet from this execution.
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
            timeout = self.default_timeout
        future = self._create_response_future(query, parameters=None, trace=trace, custom_payload=options, timeout=timeout)
        future.message._query_params = graph_parameters
        future._protocol_handler = self.client_protocol_handler
        future.row_factory = row_factory or self.default_graph_row_factory
        future.send_request()
        return future.result()

    # this may go away if we change parameter encoding
    def _transform_params(self, parameters):
        if not isinstance(parameters, dict):
            raise ValueError('The parameters must be a dictionary. Unnamed parameters are not allowed.')
        return [json.dumps({name: value}).encode('utf-8') for name, value in six.iteritems(parameters)]
