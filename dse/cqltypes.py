# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

import io
from itertools import chain
import six
from six.moves import range
import struct
from cassandra.cqltypes import CassandraType
from dse.marshal import point_be, point_le
from dse.util import Point, LineString, Polygon

_little_endian_flag = 1  # we always serialize LE

_ord = ord if six.PY2 else lambda x: x


class WKBGeometryType(object):
    POINT = 1
    LINESTRING = 2
    POLYGON = 3


class PointType(CassandraType):
    typename = 'PointType'

    _type = struct.pack('<BI', _little_endian_flag, WKBGeometryType.POINT)

    @staticmethod
    def serialize(val, protocol_version):
        return PointType._type + point_le.pack(val.x, val.y)

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(_ord(byts[0]))
        point = point_le if is_little_endian else point_be
        return Point(*point.unpack_from(byts, 5))  # ofs = endian byte + int type


class LineStringType(CassandraType):
    typename = 'LineStringType'

    _type = struct.pack('<BI', _little_endian_flag, WKBGeometryType.LINESTRING)

    @staticmethod
    def serialize(val, protocol_version):
        num_points = len(val.coords)
        return LineStringType._type + struct.pack('<I' + 'dd' * num_points, num_points, *(d for coords in val.coords for d in coords))

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(_ord(byts[0]))
        point = point_le if is_little_endian else point_be
        coords = ((point.unpack_from(byts, offset) for offset in range(1 + 4 + 4, len(byts), point.size)))  # start = endian + int type + int count
        return LineString(coords)


class PolygonType(CassandraType):
    typename = 'PolygonType'

    _type = struct.pack('<BI', _little_endian_flag, WKBGeometryType.POLYGON)
    _ring_count = struct.Struct('<I').pack

    @staticmethod
    def serialize(val, protocol_version):
        buf = io.BytesIO(PolygonType._type)
        buf.seek(0, 2)

        if val.exterior.coords:
            num_rings = 1 + len(val.interiors)
            buf.write(PolygonType._ring_count(num_rings))
            for ring in chain((val.exterior,), val.interiors):
                num_points = len(ring.coords)
                buf.write(struct.pack('<I' + 'dd' * num_points, num_points, *(d for coord in ring.coords for d in coord)))
        else:
            buf.write(PolygonType._ring_count(0))
        return buf.getvalue()

    @staticmethod
    def deserialize(byts, protocol_version):
        is_little_endian = bool(_ord(byts[0]))
        if is_little_endian:
            int_fmt = '<i'
            point = point_le
        else:
            int_fmt = '>i'
            point = point_be
        p = 5
        ring_count = struct.unpack_from(int_fmt, byts, p)[0]
        p += 4
        rings = []
        for _ in range(ring_count):
            point_count = struct.unpack_from(int_fmt, byts, p)[0]
            p += 4
            end = p + point_count * point.size
            rings.append([point.unpack_from(byts, offset) for offset in range(p, end, point.size)])
            p = end
        return Polygon(exterior=rings[0], interiors=rings[1:]) if rings else Polygon()
