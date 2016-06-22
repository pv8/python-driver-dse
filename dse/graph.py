# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

import json
import six

# (attr, description, server option)
_graph_options = (
    ('graph_name', 'name of the targeted graph.', 'graph-name'),
    ('graph_source', 'choose the graph traversal source, configured on the server side.', 'graph-source'),
    ('graph_language', 'the language used in the queries (default "gremlin-groovy")', 'graph-language'),
    ('graph_read_consistency_level', '''read `cassanddra.ConsistencyLevel <http://datastax.github.io/python-driver/api/cassandra.html#cassandra.ConsistencyLevel>`_ for graph queries (if distinct from session default).
Setting this overrides the native `Statement.consistency_level <http://datastax.github.io/python-driver/api/cassandra/query.html#cassandra.query.Statement.consistency_level>`_ for read operations from Cassandra persistence''', 'graph-read-consistency'),
    ('graph_write_consistency_level', '''write `cassandra.ConsistencyLevel <http://datastax.github.io/python-driver/api/cassandra.html#cassandra.ConsistencyLevel>`_ for graph queries (if distinct from session default).
Setting this overrides the native `Statement.consistency_level <http://datastax.github.io/python-driver/api/cassandra/query.html#cassandra.query.Statement.consistency_level>`_ for write operations to Cassandra persistence.''', 'graph-write-consistency')
)

# this is defined by the execution profile attribute, not in graph options
_request_timeout_key = 'request-timeout'


class GraphOptions(object):
    """
    Options for DSE Graph Query handler.
    """
    # See _graph_options map above for notes on valid options

    def __init__(self, **kwargs):
        self._graph_options = {}
        kwargs.setdefault('graph_source', 'g')
        kwargs.setdefault('graph_language', 'gremlin-groovy')
        for attr, value in six.iteritems(kwargs):
            setattr(self, attr, value)

    def copy(self):
        new_options = GraphOptions()
        new_options._graph_options = self._graph_options.copy()
        return new_options

    def update(self, options):
        self._graph_options.update(options._graph_options)

    def get_options_map(self, other_options=None):
        """
        Returns a map for these options updated with other options,
        and mapped to graph payload types.
        """
        options = self._graph_options.copy()
        if other_options:
            options.update(other_options._graph_options)

        # cls are special-cased so they can be enums in the API, and names in the protocol
        for cl in ('graph-write-consistency', 'graph-read-consistency'):
            cl_enum = options.get(cl)
            if cl_enum is not None:
                options[cl] = six.b(ConsistencyLevel.value_to_name[cl_enum])
        return options

    def set_source_default(self):
        """
        Sets ``graph_source`` to the server-defined default traversal source ('default')
        """
        self.graph_source = 'default'

    def set_source_analytics(self):
        """
        Sets ``graph_source`` to the server-defined analytic traversal source ('a')
        """
        self.graph_source = 'a'

    def set_source_graph(self):
        """
        Sets ``graph_source`` to the server-defined graph traversal source ('g')
        """
        self.graph_source = 'g'

    @property
    def is_default_source(self):
        return self.graph_source in (b'default', None)

    @property
    def is_analytics_source(self):
        """
        True if ``graph_source`` is set to the server-defined analytics traversal source ('a')
        """
        return self.graph_source == b'a'

    @property
    def is_graph_source(self):
        """
        True if ``graph_source`` is set to the server-defined graph traversal source ('g')
        """
        return self.graph_source == b'g'


for opt in _graph_options:

    def get(self, key=opt[2]):
        return self._graph_options.get(key)

    def set(self, value, key=opt[2]):
        if value is not None:
            # normalize text here so it doesn't have to be done every time we get options map
            if isinstance(value, six.text_type) and not isinstance(value, six.binary_type):
                value = six.b(value)
            self._graph_options[key] = value
        else:
            self._graph_options.pop(key, None)

    def delete(self, key=opt[2]):
        self._graph_options.pop(key, None)

    setattr(GraphOptions, opt[0], property(get, set, delete, opt[1]))


class SimpleGraphStatement(SimpleStatement):
    """
    Simple graph statement for :meth:`.Session.execute_graph`.
    Takes the same parameters as `cassandra.query.SimpleStatement <http://datastax.github.io/python-driver/api/cassandra/query.html#cassandra.query.SimpleStatement>`_
    """
    def __init__(self, *args, **kwargs):
        super(SimpleGraphStatement, self).__init__(*args, **kwargs)


def single_object_row_factory(column_names, rows):
    """
    returns the JSON string value of graph results
    """
    return [row[0] for row in rows]


def graph_result_row_factory(column_names, rows):
    """
    Returns a :class:`dse.graph.Result` object that can load graph results and produce specific types.
    The Result JSON is deserialized and unpacked from the top-level 'result' dict.
    """
    return [Result(json.loads(row[0])['result']) for row in rows]


def graph_object_row_factory(column_names, rows):
    """
    Like :func:`~.graph_result_row_factory`, except known element types (:class:`~.Vertex`, :class:`~.Edge`) are
    converted to their simplified objects. Some low-level metadata is shed in this conversion. Unknown result types are
    still returned as :class:`dse.graph.Result`.
    """
    return _graph_object_sequence(json.loads(row[0])['result'] for row in rows)


def _graph_object_sequence(objects):
    for o in objects:
        res = Result(o)
        if isinstance(o, dict):
            typ = res.value.get('type')
            if typ == 'vertex':
                res = res.as_vertex()
            elif typ == 'edge':
                res = res.as_edge()
        yield res


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
        """
        Return a :class:`Vertex` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Vertex(self.id, self.label, self.type, self.value.get('properties', {}))
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Vertex from %r" % (self,))

    def as_edge(self):
        """
        Return a :class:`Edge` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Edge(self.id, self.label, self.type, self.value.get('properties', {}),
                        self.inV, self.inVLabel, self.outV, self.outVLabel)
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Edge from %r" % (self,))

    def as_path(self):
        """
        Return a :class:`Path` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Path(self.labels, self.objects)
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Path from %r" % (self,))


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

    def __eq__(self, other):
        return all(getattr(self, attr) == getattr(other, attr) for attr in self._attrs)

    def __str__(self):
        return str(dict((k, getattr(self, k)) for k in self._attrs))


class Vertex(Element):
    """
    Represents a Vertex element from a graph query.

    Vertex ``properties`` are extracted into a ``dict`` of property names to list of :class:`~VertexProperty` (list
    because they are always encoded that way, and sometimes have multiple cardinality; VertexProperty because sometimes
    the properties themselves have property maps).
    """

    element_type = 'vertex'

    @staticmethod
    def _extract_properties(properties):
        # vertex properties are always encoded as a list, regardless of Cardinality
        return dict((k, [VertexProperty(p['value'], p.get('properties')) for p in v]) for k, v in properties.items())

    def __repr__(self):
        properties = dict((name, [{'value': prop.value, 'properties': prop.properties} for prop in prop_list])
                          for name, prop_list in self.properties.items())
        return "%s(%r, %r, %r, %r)" % (self.__class__.__name__,
                                       self.id, self.label,
                                       self.type, properties)


class VertexProperty(object):
    """
    Vertex properties have a top-level value and an optional ``dict`` of properties.
    """

    value = None
    """
    Value of the property
    """

    properties = None
    """
    dict of properties attached to the property
    """

    def __init__(self, value, properties=None):
        self.value = value
        self.properties = properties or {}

    def __eq__(self, other):
        return isinstance(other, VertexProperty) and self.value == other.value and self.properties == other.properties

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.value, self.properties)


class Edge(Element):
    """
    Represents an Edge element from a graph query.

    Attributes match initializer parameters.
    """

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


class Path(object):
    """
    Represents a graph path.

    Labels list is taken verbatim from the results.

    Objects are either :class:`~.Result` or :class:`~.Vertex`/:class:`~.Edge` for recognized types
    """

    labels = None
    """
    List of labels in the path
    """

    objects = None
    """
    List of objects in the path
    """

    def __init__(self, labels, objects):
        self.labels = labels
        self.objects = list(_graph_object_sequence(objects))

    def __eq__(self, other):
        return self.labels == other.labels and self.objects == other.objects

    def __str__(self):
        return str({'labels': self.labels, 'objects': self.objects})

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.labels, [o.value for o in self.objects])
