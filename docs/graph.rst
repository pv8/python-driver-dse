DSE Graph Queries
=================
Use :meth:`.Session.execute_graph` for executing gremlin queries in DSE Graph. The DSE extension executes
graph queries over the Cassandra native protocol::

    from dse.cluster import Cluster
    session = Cluster().connect()

    # no graph_options graph_name should be set any time the system API is used
    # otherwise the request will fail
    session.execute_graph("system.createGraph('test')")

    # after we're done setting up system, we can set the default graph_name to use
    # for future queries using this session
    session.default_graph_options.graph_name = 'test'
    session.execute_graph('g.addV("name", "John", "age", 35)')
    id = session.execute_graph('g.addV("name", "John", "age", 35)')[0].value
    for res in session.execute_graph('g.V()'):
        print(res.type)  # 'vertex'
        print(res.properties['name'][0].value)  # 'John'
        print(res.properties['age'][0].value)  # 35

    # graph_name should be removed from the options passed if we need to interact with the system API again
    # DSP-8121
    del session.default_graph_options.graph_name
    session.execute_graph("system.dropGraph('test')")

By default (with :attr:`.Session.default_graph_row_factory` set to :func:`.graph.graph_object_row_factory`), known graph result
types are unpacked and returned as specialized types (:class:`.Vertex`, :class:`.Edge`). If the result is not one of these
types, a :class:`.graph.Result` is returned, contining the graph result parsed from JSON and removed from its outer dict.
The class has some accessor convenience methods for accessing top-level properties by name (`type`, `properties` above),
or lists by index::

    # dicts with `__getattr__` or `__getitem__`
    result = session.execute_graph("[[key_str: 'value', key_int: 3]]")[0]
    result.value  # dict
    result.key_str  # 'value'
    result.key_int  # 3
    result['key_str']  # 'value'
    result['key_int']  # 3

    # lists with `__getitem__`
    session.execute_graph('[[0, 1, 2]]')[0][1]  # 1

You can use a different row factory by setting :attr:`.Session.default_graph_row_factory` or passing it to
:meth:`.Session.execute_graph`. For example, :func:`.graph.single_object_row_factory` returns the JSON result string`,
unparsed. :func:`.graph.graph_result_row_factory` returns parsed, but unmodified results (such that all metadata is retained,
unlike :func:`.graph.graph_object_row_factory`, which sheds some as attributes and properties are unpacked). These results
also provide convenience methods for converting to known types (:meth:`~.Result.as_vertex`, :meth:`~.Result.as_edge`, :meth:`~.Result.as_path`).

Named parameters are passed in a dict to :meth:`.cluster.Session.execute_graph`::

    result_set = session.execute_graph('[a, b]', {'a': 1, 'b': 2})
    [r.value for r in result_set]  # [1, 2]

Graph options can be set in the session default (as shown in the first example) or specified per statement::

    from dse.graph import SimpleGraphStatement
    statement = SimpleGraphStatement('g.V()')
    statement.options.graph_source = 'a'  # make this query use analytics source
    session.execute_graph(statement)
