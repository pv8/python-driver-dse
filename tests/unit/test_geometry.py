try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import struct

from cassandra.cqltypes import lookup_casstype
from cassandra.protocol import MAX_SUPPORTED_VERSION
from dse.cqltypes import PointType, CircleType, LineStringType, PolygonType, WKBGeometryType
from dse.util import Point, Circle, LineString, Polygon, _LinearRing

wkb_be = 0
wkb_le = 1

protocol_versions = range(1, MAX_SUPPORTED_VERSION + 1)


class GeoTypes(unittest.TestCase):

    samples = (Point(1, 2), Circle(1, 2, 3), LineString(((1, 2), (3, 4), (5, 6))), Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)], [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)], [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]]))

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
        self._verify_both_endian(CircleType, "ddd", (WKBGeometryType.CIRCLE, 1, 2, 3), Circle(1, 2, 3))
        self._verify_both_endian(LineStringType, "Idddddd", (WKBGeometryType.LINESTRING, 3, 1, 2, 3, 4, 5, 6), LineString(((1, 2), (3, 4), (5, 6))))
        self._verify_both_endian(PolygonType, "IIdddddd", (WKBGeometryType.POLYGON, 1, 3, 1, 2, 3, 4, 5, 6), Polygon(((1, 2), (3, 4), (5, 6))))

    def test_str_wkt(self):
        self.assertEqual(str(Point(1., 2.)), 'POINT(1.0 2.0)')
        self.assertEqual(str(Circle(1., 2., 3.)), 'CIRCLE((1.0 2.0) 3.0)')
        self.assertEqual(str(LineString(((1., 2.), (3., 4.), (5., 6.)))), 'LINESTRING(1.0 2.0, 3.0 4.0, 5.0 6.0)')
        self.assertEqual(str(_LinearRing(((1., 2.), (3., 4.), (5., 6.)))), 'LINEARRING(1.0 2.0, 3.0 4.0, 5.0 6.0)')
        self.assertEqual(str(Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                                     [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                                      [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])),
                         'POLYGON((10.1 10.0, 110.0 10.0, 110.0 110.0, 10.0 110.0, 10.0 10.0), (20.0 20.0, 20.0 30.0, 30.0 30.0, 30.0 20.0, 20.0 20.0), (40.0 20.0, 40.0 30.0, 50.0 30.0, 50.0 20.0, 40.0 20.0))')

    def test_repr(self):
        for geo in (Point(1., 2.),
                    Circle(1., 2., 3.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            self.assertEqual(eval(repr(geo)), geo)
