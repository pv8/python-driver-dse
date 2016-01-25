from itertools import chain

class Point(object):

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y

    def __hash__(self):
        return hash((self.x, self.y))

    def __str__(self):
        return "POINT(%r %r)" % (self.x, self.y)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.x, self.y)


class Circle(object):

    def __init__(self, x, y, r):
        self.x = x
        self.y = y
        self.r = r

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y and self.r == other.r

    def __hash__(self):
        return hash((self.x, self.y, self.r))

    def __str__(self):
        return "CIRCLE((%r %r) %r)" % (self.x, self.y, self.r)

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.x, self.y, self.r)


class LineString(object):

    def __init__(self, coords):
        self.coords = tuple(coords)

    def __eq__(self, other):
        return self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        return "LINESTRING(%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class _LinearRing(object):
    # no validation, no implicit closing; just used for poly composition, to
    # mimic that of shapely.geometry.Polygon
    def __init__(self, coords):
        self.coords = tuple(coords)

    def __eq__(self, other):
        return self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        return "LINEARRING(%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class Polygon(object):

    def __init__(self, exterior, interiors=None):
        self.exterior = _LinearRing(exterior)
        self.interiors = tuple(_LinearRing(e) for e in interiors) if interiors else tuple()

    def __eq__(self, other):
        return self.exterior == other.exterior and self.interiors == other.interiors

    def __hash__(self):
        return hash((self.exterior, self.interiors))

    def __str__(self):
        rings = (ring.coords for ring in chain((self.exterior,), self.interiors))
        rings = ("(%s)" % ', '.join("%r %r" % (x, y) for x, y in ring) for ring in rings)
        return "POLYGON(%s)" % ', '.join(rings)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.exterior.coords, [ring.coords for ring in self.interiors])
