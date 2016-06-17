# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms


class HostTargetingStatement(object):
    """
    Wraps any query statement and attaches a target host, making
    it usable in a targeted LBP without modifying the user's statement.
    """
    def __init__(self, inner_statement, target_host):
            self.__class__ = type(inner_statement.__class__.__name__,
                                  (self.__class__, inner_statement.__class__),
                                  {})
            self.__dict__ = inner_statement.__dict__
            self.target_host = target_host
