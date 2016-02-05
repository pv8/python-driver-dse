# Copyright 2016 DataStax, Inc.

from cassandra.query import SimpleStatement

import json
import six

# (attr, description, server option)
_graph_options = (
    ('graph_name', 'name of the targeted graph.', 'graph-name'),
    ('graph_source', 'choose the graph traversal source, configured on the server side.', 'graph-source'),
    ('graph_language', 'the language used in the queries (default "gremlin-groovy"', 'graph-language'),
    ('graph_alias', 'name of the graph in the query (default "g")', 'graph-alias')
)
# ^ mapped this way from early implementations where we were translating terms that could have been confusing to users
# Now, the options have been renamed so there is less to translate (could just be '_' -> '-'. Just leaving
# until there is an actual reason to change.


class GraphOptions(object):
    """
    Options for DSE Graph Query handler.
    See _graph_options map above for notes on valid options
    """

    def __init__(self, **kwargs):
        self._graph_options = {}
        for attr, value in six.iteritems(kwargs):
            setattr(self, attr, value)

    def update(self, options):
        self._graph_options.update(options._graph_options)

    def get_options_map(self, base_options):
        """
        Returns a map for base_options updated with options set on this object, or
        base_options map if none were set.
        """
        if self._graph_options:
            options = base_options._graph_options.copy()
            options.update(self._graph_options)
            return options
        else:
            return base_options._graph_options


for opt in _graph_options:

    def get(self, key=opt[2]):
        return self._graph_options.get(key)

    def set(self, value, key=opt[2]):
        if value:
            if not isinstance(value, six.binary_type):
                value = six.b(value)
            self._graph_options[key] = value
        else:
            self._graph_options.pop(key, None)

    def delete(self, key=opt[2]):
        self._graph_options.pop(key, None)

    setattr(GraphOptions, opt[0], property(get, set, delete, opt[1]))


class SimpleGraphStatement(SimpleStatement):
    """
    Simple graph statement for dse.cluster.Session.execute_graph.
    Takes the same parameters as cassandra.query.SimpleStatement
    """

    options = None
    """
    GraphOptions for this statement.
    Any attributes set here override the GraphSession defaults.
    """

    def __init__(self, *args, **kwargs):
        super(SimpleGraphStatement, self).__init__(*args, **kwargs)
        self.options = GraphOptions()


def single_object_row_factory(column_names, rows):
    """
    returns the JSON string value of graph results
    """
    return [row[0] for row in rows]


def graph_result_row_factory(column_names, rows):
    """
    Returns an object that can load graph results and produce specific types.
    The Result JSON is deserialized and unpacked from the top-level 'result' dict.
    """
    return [Result(json.loads(row[0])['result']) for row in rows]


class Result(object):
    """
    Represents deserialized graph results.
    Property and item getters are provided for convenience.
    """

    value = None
    """
    Deserialized value from the result
    """

    def __init__(self, value):
        self.value = value

    def __getattr__(self, attr):
        if not isinstance(self.value, dict):
            raise ValueError("Value cannot be accessed as a dict")

        if attr in self.value:
            return self.value[attr]

        raise AttributeError("Result has no top-level attribute %r" % (attr,))

    def __getitem__(self, item):
        if isinstance(self.value, dict) and isinstance(item, six.string_types):
            return self.value[item]
        elif isinstance(self.value, list) and isinstance(item, int):
            return self.value[item]
        else:
            raise ValueError("Result cannot be indexed by %r" % (item,))

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "%s(%r)" % (Result.__name__, self.value)

    def __eq__(self, other):
        return self.value == other.value

    def as_vertex(self):
        try:
            return Vertex(self.id, self.label, self.type, self.value.get('properties', {}))
        except (AttributeError, ValueError, TypeError):
            import traceback
            traceback.print_exc()
            raise TypeError("Could not create Vertex from %r" % (self,))

    def as_edge(self):
        try:
            return Edge(self.id, self.label, self.type, self.value.get('properties', {}),
                        self.inV, self.inVLabel, self.outV, self.outVLabel)
        except (AttributeError, ValueError, TypeError):
            import traceback
            traceback.print_exc()
            raise TypeError("Could not create Edge from %r" % (self,))



class Element(object):

    element_type = None

    _attrs = ('id', 'label', 'type', 'properties')

    def __init__(self, id, label, type, properties):
        if type != self.element_type:
            raise TypeError("Attempted to create %s from %s element", (type, self.element_type))

        self.id = id
        self.label = label
        self.type = type
        self.properties = self._extract_properties(properties)

    @staticmethod
    def _extract_properties(properties):
        return dict(properties)

    def __str__(self):
        return str(dict((k, getattr(self, k)) for k in self._attrs))

    def __repr__(self):
        return "%s(%r, %r, %r, %r)" % (self.__class__.__name__,
                                       self.id, self.label,
                                       self.type, self.properties)


class Vertex(Element):
    element_type = 'vertex'

    @staticmethod
    def _extract_properties(properties):
        # I have no idea why these properties are in a dict in a single-item list :-/
        return dict((k, v[0]['value']) for k, v in properties.items())


class Edge(Element):
    element_type = 'edge'

    _attrs = Element._attrs + ('inV', 'inVLabel', 'outV', 'outVLabel')

    def __init__(self, id, label, type, properties,
                 inV, inVLabel, outV, outVLabel):
        super(Edge, self).__init__(id, label, type, properties)
        self.inV = inV
        self.inVLabel = inVLabel
        self.outV = outV
        self.outVLabel = outVLabel

    def __repr__(self):
        return "%s(%r, %r, %r, %r, %r, %r, %r, %r)" %\
               (self.__class__.__name__,
                self.id, self.label,
                self.type, self.properties,
                self.inV, self.inVLabel,
                self.outV, self.outVLabel)
