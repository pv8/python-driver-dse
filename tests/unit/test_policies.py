# Copyright 2016 DataStax, Inc.
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from mock import Mock

from cassandra.pool import Host
from cassandra.policies import RoundRobinPolicy

from dse.policies import DSELoadBalancingPolicy


class ClusterMetaMock(object):
    def __init__(self, hosts=None):
        self.hosts = hosts or {}

    def get_host(self, addr):
        return self.hosts.get(addr)


class DSELoadBalancingPolicyTest(unittest.TestCase):

    def test_no_target(self):
        node_count = 4
        hosts = list(range(node_count))
        policy = DSELoadBalancingPolicy(RoundRobinPolicy())
        policy.populate(Mock(metadata=ClusterMetaMock()), hosts)
        for _ in range(node_count):
            query_plan = list(policy.make_query_plan(None, Mock(target_host=None)))
            self.assertEqual(sorted(query_plan), hosts)

    def test_status_updates(self):
        node_count = 4
        hosts = list(range(node_count))
        policy = DSELoadBalancingPolicy(RoundRobinPolicy())
        policy.populate(Mock(metadata=ClusterMetaMock()), hosts)
        policy.on_down(0)
        policy.on_remove(1)
        policy.on_up(4)
        policy.on_add(5)
        query_plan = list(policy.make_query_plan())
        self.assertEqual(sorted(query_plan), [2, 3, 4, 5])

    def test_no_live_nodes(self):
        hosts = [0, 1, 2, 3]
        policy = RoundRobinPolicy()
        policy.populate(None, hosts)

        for i in range(4):
            policy.on_down(i)

        query_plan = list(policy.make_query_plan())
        self.assertEqual(query_plan, [])

    def test_target_no_host(self):
        node_count = 4
        hosts = list(range(node_count))
        policy = DSELoadBalancingPolicy(RoundRobinPolicy())
        policy.populate(Mock(metadata=ClusterMetaMock()), hosts)
        query_plan = list(policy.make_query_plan(None, Mock(target_host='127.0.0.1')))
        self.assertEqual(sorted(query_plan), hosts)

    def test_target_host_down(self):
        node_count = 4
        hosts = [Host(i, Mock()) for i in range(node_count)]
        target_host = hosts[1]

        policy = DSELoadBalancingPolicy(RoundRobinPolicy())
        policy.populate(Mock(metadata=ClusterMetaMock({'127.0.0.1': target_host})), hosts)
        query_plan = list(policy.make_query_plan(None, Mock(target_host='127.0.0.1')))
        self.assertEqual(sorted(query_plan), hosts)

        target_host.is_up = False
        policy.on_down(target_host)
        query_plan = list(policy.make_query_plan(None, Mock(target_host='127.0.0.1')))
        self.assertNotIn(target_host, query_plan)

    def test_target_host_nominal(self):
        node_count = 4
        hosts = [Host(i, Mock()) for i in range(node_count)]
        target_host = hosts[1]
        target_host.is_up = True

        policy = DSELoadBalancingPolicy(RoundRobinPolicy())
        policy.populate(Mock(metadata=ClusterMetaMock({'127.0.0.1': target_host})), hosts)
        for _ in range(10):
            query_plan = list(policy.make_query_plan(None, Mock(target_host='127.0.0.1')))
            self.assertEqual(sorted(query_plan), hosts)
            self.assertEqual(query_plan[0], target_host)
