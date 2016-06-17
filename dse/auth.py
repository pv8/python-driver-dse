# Copyright 2016 DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
import socket

try:
    import kerberos
    _have_kerberos = True
except ImportError:
    _have_kerberos = False

try:
    from puresasl.client import SASLClient
    _have_puresasl = True
except ImportError:
    _have_puresasl = False

from cassandra.auth import AuthProvider, Authenticator


class DSEPlainTextAuthProvider(AuthProvider):
    """
    Auth provider for Plain Text authentication. Works with legacy `PasswordAuthenticator`
    or `DseAuthenticator` if `internal` scheme is enabled.
    """
    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password

    def new_authenticator(self, host):
        return PlainTextAuthenticator(self.username, self.password)


class DSEGSSAPIAuthProvider(AuthProvider):
    """
    Auth provider for GSS API authentication. Works with legacy `KerberosAuthenticator`
    or `DseAuthenticator` if `kerberos` scheme is enabled.
    """
    def __init__(self, service='dse', qops=('auth',), resolve_host_name=True, **properties):
        """
        :param service: name of the service
        :param qops: iterable of "Quality of Protection" allowed; see ``puresasl.QOP``
        :param resolve_host_name: boolean flag indicating whether the authenticator should reverse-lookup an FQDN when
            creating a new authenticator. Default is ``True``, which will resolve, or return the numeric address if there is no PTR
            record. Setting ``False`` creates the authenticator with the numeric address known by Cassandra
        :param properties: additional keyword properties to pass for the ``puresasl.mechanisms.GSSAPIMechanism`` class.
            Presently, 'principal' (user) is the only one referenced in the ``pure-sasl`` implementation
        """
        if not _have_puresasl:
            raise ImportError('The puresasl library has not been installed')
        if not _have_kerberos:
            raise ImportError('The kerberos library has not been installed')
        self.service = service
        self.qops = qops
        self.resolve_host_name = resolve_host_name
        self.properties = properties

    def new_authenticator(self, host):
        if self.resolve_host_name:
            host = socket.getnameinfo((host, 0), 0)[0]
        return GSSAPIAuthenticator(host, self.service, self.qops, self.properties)


class BaseDSEAuthenticator(Authenticator):
    def get_mechanism(self):
        raise NotImplementedError("get_mechanism not implemented")

    def get_initial_challenge(self):
        raise NotImplementedError("get_initial_challenge not implemented")

    def initial_response(self):
        if self.server_authenticator_class == "com.datastax.bdp.cassandra.auth.DseAuthenticator":
            return self.get_mechanism()
        else:
            return self.evaluate_challenge(self.get_initial_challenge())


class PlainTextAuthenticator(BaseDSEAuthenticator):
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def get_mechanism(self):
        return "PLAIN"

    def get_initial_challenge(self):
        return "PLAIN-START"

    def evaluate_challenge(self, challenge):
        if challenge == 'PLAIN-START':
            return "\x00%s\x00%s" % (self.username, self.password)
        raise Exception('Did not receive a valid challenge response from server')


class GSSAPIAuthenticator(BaseDSEAuthenticator):
    def __init__(self, host, service, qops, properties):
        properties = properties or {}
        self.sasl = SASLClient(host, service, 'GSSAPI', qops=qops, **properties)

    def get_mechanism(self):
        return "GSSAPI"

    def get_initial_challenge(self):
        return "GSSAPI-START"

    def evaluate_challenge(self, challenge):
        if challenge == 'GSSAPI-START':
            return self.sasl.process()
        else:
            return self.sasl.process(challenge)
