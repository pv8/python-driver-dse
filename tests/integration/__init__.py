# Copyright 2016 DataStax, Inc.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import os
from os.path import expanduser
from integration import PROTOCOL_VERSION, get_server_versions, BasicKeyspaceUnitTestCase, drop_keyspace_shutdown_cluster
from integration import teardown_package as base_teardown
from dse.cluster import Cluster

from integration import use_single_node
home = expanduser('~')

# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)

def teardown_package():
    base_teardown()

def use_single_node_with_graph(start=True, workloads=[]):
    use_single_node(start=start, workloads=['graph'])

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
        self.session.default_graph_options.graph_name = None
        exists = s.execute_graph('system.graphExists(name)', {'name': self.graph_name})[0].value
        if exists:
            s.execute_graph('system.dropGraph(name)', {'name': self.graph_name})


class BasicGeometricUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This base test class is used by all the geomteric tests. It contains class level teardown and setup
    methods. It also contains the test fixtures used by those tests
    """
    @classmethod
    def common_dse_setup(cls, rf, keyspace_creation=True):
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        if keyspace_creation:
            cls.create_keyspace(rf)
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.session.set_keyspace(cls.ks_name)

    @classmethod
    def setUpClass(cls):
        cls.common_dse_setup(1)
        cls.initalizeTables()

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster(cls.ks_name, cls.session, cls.cluster)

    @classmethod
    def initalizeTables(cls):
        udt_type = "CREATE TYPE udt1 (g {0})".format(cls.cql_type_name)
        large_table = "CREATE TABLE tbl (k uuid PRIMARY KEY, g {0}, l list<{0}>, s set<{0}>, m0 map<{0},int>, m1 map<int,{0}>, t tuple<{0},{0},{0}>, u frozen<udt1>)".format(cls.cql_type_name)
        simple_table = "CREATE TABLE tblpk (k {0} primary key, v int)".format( cls.cql_type_name)
        cluster_table = "CREATE TABLE tblclustering (k0 int, k1 {0}, v int, primary key (k0, k1))".format(cls.cql_type_name)
        cls.session.execute(udt_type)
        cls.session.execute(large_table)
        cls.session.execute(simple_table)
        cls.session.execute(cluster_table)
