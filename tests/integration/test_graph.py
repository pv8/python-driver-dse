# Copyright 2016 DataStax, Inc.
import time
from tests.integration import BasicGraphUnitTestCase, use_single_node_with_graph, use_singledc_wth_graph, generate_classic

import json
import six

from cassandra import OperationTimedOut, ConsistencyLevel, InvalidRequest
from cassandra.protocol import ServerError, SyntaxException
from cassandra.query import QueryTrace
from dse.graph import (SimpleGraphStatement, graph_object_row_factory, single_object_row_factory,\
                       graph_result_row_factory, Result, Edge, Vertex, Path, GraphOptions, _graph_options)


def setup_module():
    use_single_node_with_graph()


class BasicGraphTest(BasicGraphUnitTestCase):

    def test_basic_query(self):
        """
        Test to validate that basic graph query results can be executed with a sane result set.

        Creates a simple classic tinkerpot graph, and attempts to find all vertices
        related the vertex marco, that have a label of knows.
        See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should find two vertices related to marco via 'knows' edges.

        @test_category dse graph
        """
        generate_classic(self.session)
        rs = self.session.execute_graph('''g.V().has('name','marko').out('knows').values('name')''')
        self.assertFalse(rs.has_more_pages)
        results_list = [result.value for result in rs.current_rows]
        self.assertEqual(len(results_list), 2)
        self.assertIn('vadas', results_list)
        self.assertIn('josh', results_list)

    def test_classic_graph(self):
        """
        Test to validate that basic graph generation, and vertex and edges are surfaced correctly

        Creates a simple classic tinkerpot graph, and iterates over the the vertices and edges
        ensureing that each one is correct. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """
        generate_classic(self.session)
        rs = self.session.execute_graph('g.V()')
        for vertex in rs:
            self._validate_classic_vertex(vertex)
        rs = self.session.execute_graph('g.E()')
        for edge in rs:
            self._validate_classic_edge(edge)

    def test_graph_classic_path(self):
        """
        Test to validate that the path version of the result type is generated correctly. It also
        tests basic path results as that is not covered elsewhere

        @since 1.0.0
        @jira_ticket PYTHON-479
        @expected_result path object should be unpacked correctly including all nested edges and verticies
        @test_category dse graph
        """
        generate_classic(self.session)

        rs = self.session.execute_graph("g.V().hasLabel('person').has('name', 'marko').as('a')" +
            ".outE('knows').inV().as('c', 'd').outE('created').as('e', 'f', 'g').inV().path()");
        rs_list = list(rs)
        self.assertEqual(len(rs_list), 2)
        for result in rs_list:
            path = result.as_path()
            self._validate_path_result_type(path)

    def test_large_create_script(self):
        """
        Test to validate that server errors due to large groovy scripts are properly surfaced

        Creates a very large line graph script and executes it. Then proceeds to create a line graph script
        that is to large for the server to handle expects a server error to be returned

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """
        query_to_run = self._generate_line_graph(250)
        self.session.execute_graph(query_to_run)
        query_to_run = self._generate_line_graph(300)
        self.assertRaises(SyntaxException, self.session.execute_graph, query_to_run)

    def test_range_query(self):
        """
        Test to validate range queries are handled correctly.

        Creates a very large line graph script and executes it. Then proceeds to to a range
        limited query against it, and ensure that the results are formated correctly and that
        the result set is properly sized.

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result result set should be properly formated and properly sized

        @test_category dse graph
        """
        query_to_run = self._generate_line_graph(250)
        self.session.execute_graph(query_to_run)
        rs = self.session.execute_graph("g.E().range(0,10)")
        self.assertFalse(rs.has_more_pages)
        results = list(rs)
        self.assertEqual(len(results), 10)
        for result in results:
            self._validate_line_edge(result)

    def test_result_types(self):
        """
        Test to validate that the edge and vertex version of results are constructed correctly.

        @since 1.0.0
        @jira_ticket PYTHON-479
        @expected_result edge/vertex result types should be unpacked correctly.
        @test_category dse graph
        """
        self._generate_multi_field_graph()  # TODO: we could just make a single vertex with properties of all types, or even a simple query that just uses a sequence of groovy expressions

        rs = self.session.execute_graph("g.V()", row_factory=graph_result_row_factory)  # requires simplified row factory to avoid shedding id/~type information used for validation below

        for result in rs:
            self._validate_type(result)

    def test_large_result_set(self):
        """
        Test to validate that large result sets return correctly.

        Creates a very large graph. Ensures that large result sets are handled appropriately.

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result when limits of result sets are hit errors should be surfaced appropriately

        @test_category dse graph
        """
        self._generate_large_complex_graph(5000)
        rs = self.session.execute_graph("g.V()")
        for result in rs:
            self._validate_generic_vertex_result_type(result)

    def test_parameter_passing(self):
        """
        Test to validate that parameter passing works as expected

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result parameters work as expected

        @test_category dse graph
        """

        s = self.session
        # unused parameters are passed, but ignored
        s.execute_graph("null", {"doesn't": "matter", "what's": "passed"})

        # multiple params
        results = s.execute_graph("[a, b]", {'a': 0, 'b': 1})
        self.assertEqual(results[0].value, 0)
        self.assertEqual(results[1].value, 1)

        # different value types
        for param in (None, "string", 1234, 5.678, True, False):
            result = s.execute_graph('x', {'x': param})[0]
            self.assertEqual(result.value, param)

    def test_consistency_passing(self):
        """
        Test to validated that graph consistency levels are properly surfaced to the base dirver

        @since 1.0.0
        @jira_ticket PYTHON-509
        @expected_result graph consistency levels are surfaced correctly
        @test_category dse graph
        """
        cl_attrs = ('graph_read_consistency_level', 'graph_write_consistency_level')

        # Iterates over the graph options and constructs an array containing
        # The graph_options that correlate to graoh read and write consistency levels
        graph_params = [a[2] for a in _graph_options if a[0] in cl_attrs]

        s = self.session
        default_graph_opts = s.default_graph_options
        try:
            # Checks the default graph attributes and ensures that both  graph_read_consistency_level and graph_write_consistency_level
            # Are None by default
            for attr in cl_attrs:
                self.assertIsNone(getattr(default_graph_opts, attr))

            res = s.execute_graph("null")
            for param in graph_params:
                self.assertNotIn(param, res.response_future.message.custom_payload)

            # session defaults are passed
            opts = GraphOptions()
            opts.update(default_graph_opts)
            cl = {0: ConsistencyLevel.ONE, 1: ConsistencyLevel.LOCAL_QUORUM}
            for k, v in cl.items():
                setattr(opts, cl_attrs[k], v)
            s.default_graph_options = opts

            res = s.execute_graph("null")

            for k, v in cl.items():
                self.assertEqual(res.response_future.message.custom_payload[graph_params[k]], six.b(ConsistencyLevel.value_to_name[v]))

            # statement values override session defaults
            cl = {0: ConsistencyLevel.ALL, 1: ConsistencyLevel.QUORUM}
            sgs = SimpleGraphStatement("null")
            for k, v in cl.items():
                attr_name = cl_attrs[k]
                setattr(sgs.options, attr_name, v)
                self.assertNotEqual(getattr(s.default_graph_options, attr_name), getattr(sgs.options, attr_name))

            res = s.execute_graph(sgs)

            for k, v in cl.items():
                self.assertEqual(res.response_future.message.custom_payload[graph_params[k]], six.b(ConsistencyLevel.value_to_name[v]))
        finally:
            s.default_graph_options = default_graph_opts

    def test_geometric_graph_types(self):
        """
        Test to validate that geometric types function correctly

        Creates a very simple graph, and tries to insert a simple point type

        @since 1.0.0
        @jira_ticket DSP-8087
        @expected_result json types assoicated with insert is parsed correctly

        @test_category dse graph
        """
        self.session.execute_graph('''import org.apache.cassandra.db.marshal.geometry.Point;
                                      schema.vertexLabel('PointV').ifNotExists().create();
                                      schema.propertyKey('pointP').Point().ifNotExists().create();''')

        rs = self.session.execute_graph('''g.addV(label, 'PointV', 'pointP', 'POINT(0 1)');''')

        # if result set is not parsed correctly this will throw an exception
        self.assertIsNotNone(rs)

    def test_result_forms(self):
        """
        Test to validate that geometric types function correctly

        Creates a very simple graph, and tries to insert a simple point type

        @since 1.0.0
        @jira_ticket DSP-8087
        @expected_result json types assoicated with insert is parsed correctly

        @test_category dse graph
        """
        generate_classic(self.session)
        rs = list(self.session.execute_graph('g.V()'))
        self.assertGreater(len(rs), 0, "Result set was empty this was not expected")
        for v in rs:
            self._validate_classic_vertex(v)

        rs = list(self.session.execute_graph('g.E()'))
        self.assertGreater(len(rs), 0, "Result set was empty this was not expected")
        for e in rs:
            self._validate_classic_edge(e)

    def test_vertex_multiple_properties(self):
        """
        Test verifying vertex property form for various Cardinality

        All key types are encoded as a list, regardless of cardinality

        Single cardinality properties have only one value -- the last one added

        Default is single (this is config dependent)

        @since 1.0.0
        @jira_ticket PYTHON-487

        @test_category dse graph
        """
        s = self.session
        s.execute_graph('''Schema schema = graph.schema();
                           schema.propertyKey('mult_key').Text().multiple().ifNotExists().create();
                           schema.propertyKey('single_key').Text().single().ifNotExists().create();''')

        # multiple_with_one_value
        v = s.execute_graph("graph.addVertex('mult_key', 'value')")[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['mult_key']), 1)
        self.assertEqual(v.properties['mult_key'][0].value, 'value')

        # multiple_with_two_values
        v = s.execute_graph("graph.addVertex('mult_key', 'value0', 'mult_key', 'value1')")[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['mult_key']), 2)
        self.assertEqual(v.properties['mult_key'][0].value, 'value0')
        self.assertEqual(v.properties['mult_key'][1].value, 'value1')

        # single_with_one_value
        v = s.execute_graph("graph.addVertex('single_key', 'value')")[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['single_key']), 1)
        self.assertEqual(v.properties['single_key'][0].value, 'value')

        # single_with_two_values
        with self.assertRaises(InvalidRequest):
            v = s.execute_graph("graph.addVertex('single_key', 'value0', 'single_key', 'value1')")[0]

        # default_with_two_values
        with self.assertRaises(InvalidRequest):
            v = s.execute_graph("graph.addVertex('default_key', 'value0', 'default_key', 'value1')")[0]

    def test_vertex_property_properties(self):
        """
        Test verifying vertex property properties

        @since 1.0.0
        @jira_ticket PYTHON-487

        @test_category dse graph
        """
        s = self.session

        v = s.execute_graph('''v = graph.addVertex()
                                 v.property('key', 'value', 'k0', 'v0', 'k1', 'v1')
                                 v''')[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['key']), 1)
        p = v.properties['key'][0]
        self.assertEqual(p.value, 'value')
        self.assertEqual(p.properties, {'k0': 'v0', 'k1': 'v1'})

    def test_statement_graph_options(self):
        s = self.session
        statement = SimpleGraphStatement("true")
        statement.options.graph_name = self.graph_name
        self.assertTrue(s.execute_graph(statement)[0].value)

        # bad graph name to verify it's passed
        statement.options.graph_name = "definitely_not_correct"
        self.assertRaises(ServerError, s.execute_graph, statement)

        # removing makes it use the correct default
        del statement.options.graph_name
        self.assertTrue(s.execute_graph(statement)[0].value)

    def test_execute_graph_timeout(self):
        s = self.session

        value = [1, 2, 3]
        query = "[%r]" % (value,)

        # default is passed down
        rs = s.execute_graph(query)
        self.assertEqual(rs[0].value, value)
        self.assertEqual(rs.response_future.timeout, s.default_graph_timeout)

        # tiny timeout times out as expected
        self.assertRaises(OperationTimedOut, s.execute_graph, query, timeout=0.0001)

    def test_execute_graph_trace(self):
        s = self.session

        value = [1, 2, 3]
        query = "[%r]" % (value,)

        # default is no trace
        rs = s.execute_graph(query)
        self.assertEqual(rs[0].value, value)
        self.assertIsNone(rs.get_query_trace())

        # request trace
        rs = s.execute_graph(query, trace=True)
        self.assertEqual(rs[0].value, value)
        qt = rs.get_query_trace(max_wait_sec=10)
        self.assertIsInstance(qt, QueryTrace)
        self.assertIsNotNone(qt.duration)

    def test_execute_graph_row_factory(self):
        s = self.session

        # default Results
        self.assertEqual(s.default_graph_row_factory, graph_object_row_factory)
        result = s.execute_graph("123")[0]
        self.assertIsInstance(result, Result)
        self.assertEqual(result.value, 123)

        # other via parameter
        rs = s.execute_graph("123", row_factory=single_object_row_factory)
        self.assertEqual(rs.response_future.row_factory, single_object_row_factory)
        self.assertEqual(json.loads(rs[0]), {'result': 123})

    def _validate_type(self, vertex):
        for properties in vertex.properties.values():
            prop = properties[0]
            type_indicator = prop['id']['~type']
            if any(type_indicator.startswith(t) for t in ('int', 'short', 'long')):
                typ = int
            elif any(type_indicator.startswith(t) for t in ('float', 'double')):
                typ = float
            else:
                self.fail("Received unexpected type: %s" % type_indicator)
            self.assertIsInstance(prop['value'], typ)

    def _validate_classic_vertex(self, vertex):
        vertex_props = vertex.properties.keys()
        self.assertEqual(len(vertex_props), 2)
        self.assertIn('name', vertex_props)
        self.assertTrue('lang' in vertex_props or 'age' in vertex_props)

    def _validate_classic_vertex_return_type(self, vertex):
        self._validate_generic_vertex_result_type(vertex)
        vertex_props = vertex.properties
        self.assertIn('name', vertex_props)
        self.assertTrue('lang' in vertex_props or 'age' in vertex_props)

    def _validate_generic_vertex_result_type(self, vertex):
        self.assertIsInstance(vertex, Vertex)
        for attr in ('id', 'type', 'label', 'properties'):
            self.assertIsNotNone(getattr(vertex, attr))

    def _validate_classic_edge_properties(self, edge_properties):
        self.assertEqual(len(edge_properties.keys()), 1)
        self.assertIn('weight', edge_properties)

    def _validate_classic_edge(self, edge):
        self._validate_generic_edge_result_type(edge)
        self._validate_classic_edge_properties(edge.properties)

    def _validate_line_edge(self, edge):
        self._validate_generic_edge_result_type(edge)
        edge_props = edge.properties
        self.assertEqual(len(edge_props.keys()), 1)
        self.assertIn('distance', edge_props)

    def _validate_generic_edge_result_type(self, edge):
        self.assertIsInstance(edge, Edge)
        for attr in ('properties', 'outV', 'outVLabel', 'inV', 'inVLabel', 'label', 'type', 'id'):
            self.assertIsNotNone(getattr(edge, attr))

    def _validate_path_result_type(self, path):
        self.assertIsInstance(path, Path)
        self.assertIsNotNone(path.labels)
        for obj in path.objects:
            if isinstance(obj, Edge):
                self._validate_classic_edge(obj)
            elif isinstance(obj, Vertex):
                self._validate_classic_vertex(obj)
            else:
                self.fail("Invalid object found in path " + str(object.type))

    def _generate_line_graph(self, length):
        query_parts = []
        for index in range(0, length):
            query_parts.append('''Vertex vertex{0} = graph.addVertex("index", {0}); '''.format(index))
            if index is not 0:
                query_parts.append('''vertex{0}.addEdge("goesTo", vertex{1}, "distance", 5); '''.format(index-1,index))
        final_graph_generation_statement = "".join(query_parts)
        return final_graph_generation_statement

    def _generate_multi_field_graph(self):
        to_run = ['''short s1 = 5000; graph.addVertex(label, "shortvertex", "shortvalue", s1);''',
                 '''int i1 = 1000000000; graph.addVertex(label, "intvertex", "intvalue", i1);''',
                 '''Integer i2 = 100000000; graph.addVertex(label, "intvertex2", "intvalue2", i2);''',
                 '''long l1 = 9223372036854775807; graph.addVertex(label, "longvertex", "longvalue", l1);''',
                 '''Long l2 = 100000000000000000L; graph.addVertex(label, "longvertex2", "longvalue2", l2);''',
                 '''float f1 = 3.5f; graph.addVertex(label, "floatvertex", "floatvalue", f1);''',
                 '''double d1 = 3.5e40; graph.addVertex(label, "doublevertex", "doublevalue", d1);''',
                 '''Double d2 = 3.5e40d; graph.addVertex(label, "doublevertex2", "doublevalue2", d2);''']

        for run in to_run:
            self.session.execute_graph(run)

    def _generate_large_complex_graph(self, size):

        to_run ='''int size = 2000;
            List ids = new ArrayList();
            Vertex v = graph.addVertex();
            v.property("ts", 100001);
            v.property("sin", 0);
            v.property("cos", 1);
            v.property("ii", 0);
            ids.add(v.id());
            Random rand = new Random();
            for (int ii = 1; ii < size; ii++) {
                v = graph.addVertex();
                v.property("ii", ii);
                v.property("ts", 100001 + ii);
                v.property("sin", Math.sin(ii/5.0));
                v.property("cos", Math.cos(ii/5.0));
                Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();
                v.addEdge("linked", u);
                ids.add(u.id());
                ids.add(v.id());
            }
            g.V().count();'''
        self.session.execute_graph(to_run, timeout=32)
