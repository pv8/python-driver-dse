# Copyright 2016 DataStax, Inc.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import os
from os.path import expanduser
from dse.cluster import Cluster
from integration import PROTOCOL_VERSION, get_server_versions, BasicKeyspaceUnitTestCase

home = expanduser('~')

# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)


class BasicGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """
    @property
    def graph_name(self):
        return self._testMethodName.lower()

    def session_setup(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        self.session_setup()
        self.reset_graph()
        self.session.default_graph_options.graph_name = self.graph_name

    def tearDown(self):
        self.drop_graph()
        self.cluster.shutdown()

    def reset_graph(self):
        self.drop_graph()
        self.session.execute_graph('system.createGraph(name).build()', {'name': self.graph_name})

    def drop_graph(self):
        s = self.session
        # might also g.V().drop().iterate(), but that leaves some schema behind
        # this seems most robust for now
        exists = s.execute_graph('system.graphExists(name)', {'name': self.graph_name})[0].value
        if exists:
            s.execute_graph('system.dropGraph(name)', {'name': self.graph_name})
