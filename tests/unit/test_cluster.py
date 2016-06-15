# Copyright 2016 DataStax, Inc.

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from dse import _core_driver_target_version
from dse.cluster import Cluster

from mock import patch


class ClusterTests(unittest.TestCase):

    @patch('dse.cluster.core_driver_version', "0.0.1")
    def test_version_validation_invalid(self):
        """
        Ensure cluster creation fails when invalid driver dependency is found

        @since 1.0.0
        @jira_ticket PYTHON-568
        @expected_result runtime error

        @test_category cluster
        """
        with self.assertRaises(RuntimeError):
            Cluster()

    @patch('dse.cluster.core_driver_version', _core_driver_target_version)
    def test_verion_validation_valid(self):
        """
        Ensure cluster creation succeeds when valid driver dependency is found

        @since 1.0.0
        @jira_ticket PYTHON-568
        @expected_result success

        @test_category cluster
        """
        Cluster()
