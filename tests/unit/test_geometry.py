# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import struct

from cassandra.cqltypes import lookup_casstype
from cassandra.protocol import MAX_SUPPORTED_VERSION
from dse.cqltypes import PointType, LineStringType, PolygonType, WKBGeometryType
from dse.util import Point, LineString, Polygon, _LinearRing

wkb_be = 0
wkb_le = 1

protocol_versions = range(1, MAX_SUPPORTED_VERSION + 1)


class GeoTypes(unittest.TestCase):

    samples = (Point(1, 2), LineString(((1, 2), (3, 4), (5, 6))), Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)], [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)], [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]]))

    def test_marshal_platform(self):
        for proto_ver in protocol_versions:
            for geo in self.samples:
                cql_type = lookup_casstype(geo.__class__.__name__ + 'Type')
                self.assertEqual(cql_type.from_binary(cql_type.to_binary(geo, proto_ver), proto_ver), geo)

    def _verify_both_endian(self, typ, body_fmt, params, expected):
        for proto_ver in protocol_versions:
            self.assertEqual(typ.from_binary(struct.pack(">BI" + body_fmt, wkb_be, *params), proto_ver), expected)
            self.assertEqual(typ.from_binary(struct.pack("<BI" + body_fmt, wkb_le, *params), proto_ver), expected)

    def test_both_endian(self):
        self._verify_both_endian(PointType, "dd", (WKBGeometryType.POINT, 1, 2), Point(1, 2))
        self._verify_both_endian(LineStringType, "Idddddd", (WKBGeometryType.LINESTRING, 3, 1, 2, 3, 4, 5, 6), LineString(((1, 2), (3, 4), (5, 6))))
        self._verify_both_endian(PolygonType, "IIdddddd", (WKBGeometryType.POLYGON, 1, 3, 1, 2, 3, 4, 5, 6), Polygon(((1, 2), (3, 4), (5, 6))))

    def test_empty_wkb(self):
        for cls in (LineString, Polygon):
            class_name = cls.__name__
            cql_type = lookup_casstype(class_name + 'Type')
            self.assertEqual(str(cql_type.from_binary(cql_type.to_binary(cls(), 0), 0)), class_name.upper() + " EMPTY")
        self.assertEqual(str(PointType.from_binary(PointType.to_binary(Point(), 0), 0)), "POINT (nan nan)")

    def test_str_wkt(self):
        self.assertEqual(str(Point(1., 2.)), 'POINT (1.0 2.0)')
        self.assertEqual(str(Point()), "POINT (nan nan)")
        self.assertEqual(str(LineString(((1., 2.), (3., 4.), (5., 6.)))), 'LINESTRING (1.0 2.0, 3.0 4.0, 5.0 6.0)')
        self.assertEqual(str(_LinearRing(((1., 2.), (3., 4.), (5., 6.)))), 'LINEARRING (1.0 2.0, 3.0 4.0, 5.0 6.0)')
        self.assertEqual(str(Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                                     [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                                      [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])),
                         'POLYGON ((10.1 10.0, 110.0 10.0, 110.0 110.0, 10.0 110.0, 10.0 10.0), (20.0 20.0, 20.0 30.0, 30.0 30.0, 30.0 20.0, 20.0 20.0), (40.0 20.0, 40.0 30.0, 50.0 30.0, 50.0 20.0, 40.0 20.0))')

        class LinearRing(_LinearRing):
            pass
        for cls in (LineString, LinearRing, Polygon):
            self.assertEqual(str(cls()), cls.__name__.upper() + " EMPTY")

    def test_repr(self):
        for geo in (Point(1., 2.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            self.assertEqual(eval(repr(geo)), geo)

    def test_hash(self):
        for geo in (Point(1., 2.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            self.assertEqual(len(set((geo, geo))), 1)

    def test_eq(self):
        for geo in (Point(1., 2.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            # same type
            self.assertEqual(geo, geo)

            # does not blow up on other types
            # specifically use assertFalse(eq) to make sure we're using the geo __eq__ operator
            self.assertFalse(geo == object())

