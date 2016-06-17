# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms

from puresasl import QOP

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

from dse.auth import DSEGSSAPIAuthProvider

class TestGSSAPI(unittest.TestCase):

    def test_host_resolution(self):
        # resolves by default
        provider = DSEGSSAPIAuthProvider(service='test', qops=QOP.all)
        authenticator = provider.new_authenticator('127.0.0.1')
        self.assertEqual(authenticator.sasl.host, 'localhost')

        # numeric fallback okay
        authenticator = provider.new_authenticator('0.0.0.1')
        self.assertEqual(authenticator.sasl.host, '0.0.0.1')

        # disable explicitly
        provider = DSEGSSAPIAuthProvider(service='test', qops=QOP.all, resolve_host_name=False)
        authenticator = provider.new_authenticator('127.0.0.1')
        self.assertEqual(authenticator.sasl.host, '127.0.0.1')

