# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from itertools import chain

_nan = float('nan')


class Point(object):
    """
    Represents a point geometry for DSE
    """

    x = None
    """
    x coordinate of the point
    """

    y = None
    """
    y coordinate of the point
    """

    def __init__(self, x=_nan, y=_nan):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return isinstance(other, Point) and self.x == other.x and self.y == other.y

    def __hash__(self):
        return hash((self.x, self.y))

    def __str__(self):
        """
        Well-known text representation of the point
        """
        return "POINT (%r %r)" % (self.x, self.y)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.x, self.y)


class LineString(object):
    """
    Represents a linestring geometry for DSE
    """

    coords = None
    """
    Tuple of (x, y) coordinates in the linestring
    """
    def __init__(self, coords=tuple()):
        """
        'coords`: a sequence of (x, y) coordinates of points in the linestring
        """
        self.coords = tuple(coords)

    def __eq__(self, other):
        return isinstance(other, LineString) and self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        """
        Well-known text representation of the LineString
        """
        if not self.coords:
            return "LINESTRING EMPTY"
        return "LINESTRING (%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class _LinearRing(object):
    # no validation, no implicit closing; just used for poly composition, to
    # mimic that of shapely.geometry.Polygon
    def __init__(self, coords=tuple()):
        self.coords = tuple(coords)

    def __eq__(self, other):
        return isinstance(other, _LinearRing) and self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        if not self.coords:
            return "LINEARRING EMPTY"
        return "LINEARRING (%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class Polygon(object):
    """
    Represents a polygon geometry for DSE
    """

    exterior = None
    """
    _LinearRing representing the exterior of the polygon
    """

    interiors = None
    """
    Tuple of _LinearRings representing interior holes in the polygon
    """

    def __init__(self, exterior=tuple(), interiors=None):
        """
        'exterior`: a sequence of (x, y) coordinates of points in the linestring
        `interiors`: None, or a sequence of sequences or (x, y) coordinates of points describing interior linear rings
        """
        self.exterior = _LinearRing(exterior)
        self.interiors = tuple(_LinearRing(e) for e in interiors) if interiors else tuple()

    def __eq__(self, other):
        return isinstance(other, Polygon) and self.exterior == other.exterior and self.interiors == other.interiors

    def __hash__(self):
        return hash((self.exterior, self.interiors))

    def __str__(self):
        """
        Well-known text representation of the polygon
        """
        if not self.exterior.coords:
            return "POLYGON EMPTY"
        rings = [ring.coords for ring in chain((self.exterior,), self.interiors)]
        rings = ("(%s)" % ', '.join("%r %r" % (x, y) for x, y in ring) for ring in rings)
        return "POLYGON (%s)" % ', '.join(rings)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.exterior.coords, [ring.coords for ring in self.interiors])
