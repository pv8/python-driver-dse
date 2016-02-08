# Copyright 2016 DataStax, Inc.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import json
import six

from dse.graph import (SimpleGraphStatement, GraphOptions, Result,
                       _graph_options, graph_result_row_factory, single_object_row_factory)


class GraphResultTests(unittest.TestCase):

    _values = (None, 1, 1.2, True, False, [1, 2, 3], {'x': 1, 'y': 2})

    def test_result_value(self):
        for v in self._values:
            result = self._make_result(v)
            self.assertEqual(result.value, v)

    def test_result_attr(self):
        # value is not a dict
        result = self._make_result(123)
        with self.assertRaises(ValueError):
            result.something

        expected = {'a': 1, 'b': 2}
        result = self._make_result(expected)
        self.assertEqual(result.a, 1)
        self.assertEqual(result.b, 2)
        with self.assertRaises(AttributeError):
            result.not_present

    def test_result_item(self):
        # value is not a dict, list
        result = self._make_result(123)
        with self.assertRaises(ValueError):
            result['something']
        with self.assertRaises(ValueError):
            result[0]

        # dict key access
        expected = {'a': 1, 'b': 2}
        result = self._make_result(expected)
        self.assertEqual(result['a'], 1)
        self.assertEqual(result['b'], 2)
        with self.assertRaises(KeyError):
            result['not_present']
        with self.assertRaises(ValueError):
            result[0]

        # list index access
        expected = [0, 1]
        result = self._make_result(expected)
        self.assertEqual(result[0], 0)
        self.assertEqual(result[1], 1)
        with self.assertRaises(IndexError):
            result[2]
        with self.assertRaises(ValueError):
            result['something']

    def test_str(self):
        for v in self._values:
            self.assertEqual(str(self._make_result(v)), str(v))

    def test_repr(self):
        for v in self._values:
            result = self._make_result(v)
            self.assertEqual(eval(repr(result)), result)

    def _make_result(self, value):
        # result is always json-encoded map with 'result' item
        return Result(json.dumps({'result': value}))


class GraphOptionTests(unittest.TestCase):

    opt_mapping = dict((t[0], t[2]) for t in _graph_options)

    api_params = dict((p, str(i)) for i, p in enumerate(opt_mapping))

    def test_init(self):
        opts = GraphOptions(**self.api_params)
        self._verify_api_params(opts, self.api_params)
        self._verify_api_params(GraphOptions(), {})

    def test_update(self):
        opts = GraphOptions(**self.api_params)
        new_params = dict((k, str(int(v) + 1)) for k, v in self.api_params.items())
        opts.update(GraphOptions(**new_params))
        self._verify_api_params(opts, new_params)

    def test_get_options(self):
        # nothing set --> base map
        base = GraphOptions(**self.api_params)
        self.assertIs(GraphOptions().get_options_map(base), base._graph_options)

        # something set overrides
        other = GraphOptions(graph_name='unit_test')
        options = other.get_options_map(base)
        updated = self.opt_mapping['graph_name']
        self.assertEqual(options[updated], six.b('unit_test'))
        for name in (n for n in self.opt_mapping.values() if n != updated):
            self.assertEqual(options[name], base._graph_options[name])

        # base unchanged
        self._verify_api_params(base, self.api_params)

    def test_set_attr(self):
        expected = 'test@@@@'
        opts = GraphOptions(graph_name=expected)
        self.assertEqual(opts.graph_name, six.b(expected))
        expected = 'somethingelse####'
        opts.graph_name = expected
        self.assertEqual(opts.graph_name, six.b(expected))

        # will update options with set value
        another = GraphOptions()
        self.assertIsNone(another.graph_name)
        another.update(opts)
        self.assertEqual(another.graph_name, six.b(expected))

        opts.graph_name = None
        self.assertIsNone(opts.graph_name)
        # will not update another with its set-->unset value
        another.update(opts)
        self.assertEqual(another.graph_name, six.b(expected))  # remains unset
        opt_map = opts.get_options_map(another)
        self.assertIs(opt_map, another._graph_options)

    def test_del_attr(self):
        opts = GraphOptions(**self.api_params)
        test_params = self.api_params.copy()
        del test_params['graph_alias']
        del opts.graph_alias
        self._verify_api_params(opts, test_params)

    def _verify_api_params(self, opts, api_params):
        self.assertEqual(len(opts._graph_options), len(api_params))
        for name, value in api_params.items():
            value = six.b(value)
            self.assertEqual(getattr(opts, name), value)
            self.assertEqual(opts._graph_options[self.opt_mapping[name]], value)


class GraphStatementTests(unittest.TestCase):

    def test_init(self):
        # just make sure Statement attributes are accepted
        kwargs = {'query_string': object(),
                  'retry_policy': object(),
                  'consistency_level': object(),
                  'fetch_size': object(),
                  'keyspace': object(),
                  'custom_payload': object()}
        statement = SimpleGraphStatement(**kwargs)
        for k, v in kwargs.items():
            self.assertIs(getattr(statement, k), v)

        # but not a bogus parameter
        kwargs['bogus'] = object()
        self.assertRaises(TypeError, SimpleGraphStatement, **kwargs)


class GraphRowFactoryTests(unittest.TestCase):

    def test_object_row_factory(self):
        col_names = []  # unused
        rows = [object() for _ in range(10)]
        self.assertEqual(single_object_row_factory(col_names, ((o,) for o in rows)), rows)

    def test_graph_result_row_factory(self):
        col_names = []  # unused
        rows = [json.dumps({'result': i}) for i in range(10)]
        results = graph_result_row_factory(col_names, ((o,) for o in rows))
        for i, res in enumerate(results):
            self.assertIsInstance(res, Result)
            self.assertEqual(res.value, i)
