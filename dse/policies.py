# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
from cassandra.policies import LoadBalancingPolicy, RetryPolicy


class WrapperPolicy(LoadBalancingPolicy):

    def __init__(self, child_policy):
        self._child_policy = child_policy

    def distance(self, *args, **kwargs):
        return self._child_policy.distance(*args, **kwargs)

    def populate(self, cluster, hosts):
        self._child_policy.populate(cluster, hosts)

    def on_up(self, *args, **kwargs):
        return self._child_policy.on_up(*args, **kwargs)

    def on_down(self, *args, **kwargs):
        return self._child_policy.on_down(*args, **kwargs)

    def on_add(self, *args, **kwargs):
        return self._child_policy.on_add(*args, **kwargs)

    def on_remove(self, *args, **kwargs):
        return self._child_policy.on_remove(*args, **kwargs)


class DSELoadBalancingPolicy(WrapperPolicy):
    """
    A :class:`.LoadBalancingPolicy` wrapper that adds the ability to target a specific host first.

    If no host is set on the query, the child policy's query plan will be used as is.
    """

    _cluster_metadata = None

    def populate(self, cluster, hosts):
        self._cluster_metadata = cluster.metadata
        self._child_policy.populate(cluster, hosts)

    def make_query_plan(self, working_keyspace=None, query=None):
        if query and query.keyspace:
            keyspace = query.keyspace
        else:
            keyspace = working_keyspace

        addr = getattr(query, 'target_host', None) if query else None
        target_host = self._cluster_metadata.get_host(addr)

        child = self._child_policy
        if target_host and target_host.is_up:
            yield target_host
            for h in child.make_query_plan(keyspace, query):
                if h != target_host:
                    yield h
        else:
            for h in child.make_query_plan(keyspace, query):
                yield h


class NeverRetryPolicy(RetryPolicy):
    def _rethrow(self, *args, **kwargs):
        return self.RETHROW, None

    on_read_timeout = _rethrow
    on_write_timeout = _rethrow
    on_unavailable = _rethrow
