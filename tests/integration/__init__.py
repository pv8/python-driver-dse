# Copyright 2016 DataStax, Inc.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import os
from os.path import expanduser
from cassandra.cluster import Cluster
from integration import PROTOCOL_VERSION, get_server_versions, BasicKeyspaceUnitTestCase
from dse.graph import GraphSession

home = expanduser('~')

# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)


class BasicGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """
    @property
    def graph_name_space(self):
        return self._testMethodName.lower()

    def graph_setup(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        graph_name_space_param = {'graph-keyspace': self.ks_name}
        self.graph_session = GraphSession(self.session, graph_name_space_param)
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        self.graph_setup()
        self.drop_all_verticies()

    def tearDown(self):
        self.cluster.shutdown()

    def drop_all_verticies(self):
        return self.graph_session.execute("g.V().drop().iterate(); g.V()")

