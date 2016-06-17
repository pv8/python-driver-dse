# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from struct import Struct

point_be = Struct('>dd')
point_le = Struct('<dd')

circle_be = Struct('>ddd')
circle_le = Struct('<ddd')
