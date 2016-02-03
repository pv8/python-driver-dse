DSE Graph Queries
=================
Use :meth:`dse.cluster.Sessionx.execute_graph` for executing gremlin queries in DSE Graph. The DSE extension executes
graph queries over the Cassandra native protocol::

    from dse.cluster import Cluster
    session = Cluster().connect()
    session.execute_graph("system.createGraph('test')")  # no graph name should be set any time the system API is used
    session.default_graph_options.graph_name = 'test'  # use this default graph name for all queries
    session.execute_graph('g.addV("name", "John", "age", 35)')
    id = session.execute_graph('g.addV("name", "John", "age", 35)')[0].value
    for res in session.execute_graph('g.V()'):
        print(res.type)  # 'vertex'
        print(res.properties['name'][0]['value'])  # 'John'
        print(res.properties['age'][0]['value'])  # 35

By default (with `Session.default_graph_row_factory` set to :func:`.graph.graph_result_row_factory`), each result is a
:class:`.graph.Result`, which contains the graph result, parsed from JSON and removed from its outer dict.
The result value is not transformed in any way presently, but the class does have some accessor convenience methods for
accessing top-level properties by name (`type`, `properties` above), or lists by index::

    # dicts with `__getattr__` or `__getitem__`
    result = session.execute_graph("[[key_str: 'value', key_int: 3]]")[0]
    result.value  # dict
    result.key_str  # 'value'
    result.key_int  # 3
    result['key_str']  # 'value'
    result['key_int']  # 3

    # lists with `__getitem__`
    session.execute_graph('[[0, 1, 2]]')[0][1]  # 1

You can use a different row factory by setting :attr:`Session.default_graph_row_factory` or passing it to
:meth:`Session.execute_graph`. For example, :func:`.graph.single_object_row_factory` returns the JSON result string`,
unparsed.

Named parameters are passed in a dict to :meth:`.cluster.Session.execute_graph`::

    result_set = session.execute_graph('[a, b]', {'a': 1, 'b': 2})
    [r.value for r in result_set]  # [1, 2]

Graph options can be set in the session default (as shown in the first example) or specified per statement::

    from dse.graph import SimpleGraphStatement
    statement = SimpleGraphStatement('x.V()')  # this query refers to the graph by 'x' instead of the default 'g'
    statement.options.graph_alias = 'x'
    session.execute_graph(statement)
