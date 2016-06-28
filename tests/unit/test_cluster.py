# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from dse import _core_driver_target_version
from dse.cluster import Cluster
from dse import _use_any_core_driver_version

from mock import patch


class ClusterTests(unittest.TestCase):

    @unittest.skipUnless(not _use_any_core_driver_version, "Dependency validation is disabled")
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

    @unittest.skipUnless(not _use_any_core_driver_version, "Dependency validation is disabled")
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
