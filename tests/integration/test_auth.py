# Copyright 2016 DataStax, Inc.

from nose.plugins.attrib import attr
from dse.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement
from dse.auth import DSEGSSAPIAuthProvider
import os, time, logging
import subprocess
from tests.integration import ADS_HOME, use_single_node_with_graph, generate_classic, reset_graph

from integration import get_cluster, remove_cluster
from ccmlib.dse_cluster import DseCluster
try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa


log = logging.getLogger(__name__)


def setup_module():
    use_single_node_with_graph()


def teardown_module():
    remove_cluster()  # this test messes with config


@attr('long')
class BasicDseAuthTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """
        This will setup the necessary infrastructure to run our authentication tests. It requres the ADS_HOME environment variable
        and our custom embedded apache directory server jar in order to run.
        """

        clear_kerberos_tickets()

         # Setup variables for various keytab and other files
        self.conf_file_dir = ADS_HOME+"conf/"
        self.krb_conf = self.conf_file_dir+"krb5.conf"
        self.dse_keytab = self.conf_file_dir+"dse.keytab"
        self.dseuser_keytab = self.conf_file_dir+"dseuser.keytab"
        self.cassandra_keytab = self.conf_file_dir+"cassandra.keytab"
        actual_jar = ADS_HOME+"embedded-ads.jar"

        # Create configuration directories if they don't already exists
        if not os.path.exists(self.conf_file_dir):
            os.makedirs(self.conf_file_dir)
        log.warning("Starting adserver")
        # Start the ADS, this will create the keytab con configuration files listed above
        self.proc = subprocess.Popen(['java', '-jar', actual_jar, '-k', '--confdir', self.conf_file_dir], shell=False)
        time.sleep(10)
        #TODO poll for server to come up

        log.warning("Starting adserver started")
        ccm_cluster = get_cluster()
        log.warning("fetching tickets")
        # Stop cluster if running and configure it with the correct options
        ccm_cluster.stop()
        if isinstance(ccm_cluster, DseCluster):
            # Setup kerberos options in dse.yaml
            config_options = {'kerberos_options': {'keytab': self.dse_keytab,
                                                   'service_principal': 'dse/_HOST@DATASTAX.COM',
                                                   'qop': 'auth'},
                              'authentication_options' : {'enabled': 'true',
                                                          'default_scheme': 'kerberos',
                                                          'scheme_permissions': 'true',
                                                          'allow_digest_with_kerberos': 'true',
                                                          'plain_text_without_ssl': 'warn',
                                                          'transitional_mode': 'disabled'}
                              }

            krb5java = "-Djava.security.krb5.conf="+self.krb_conf
            # Setup dse authenticator in cassandra.yaml
            ccm_cluster.set_configuration_options({'authenticator': 'com.datastax.bdp.cassandra.auth.DseAuthenticator'})
            ccm_cluster.set_dse_configuration_options(config_options)
            ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=[krb5java])
        else:
            log.error("Cluster is not dse cluster test will fail")

    @classmethod
    def tearDownClass(self):
        """
        Terminates running ADS (Apache directory server).
        """

        self.proc.terminate()

    def tearDown(self):
        """
        This will clear any existing kerberos tickets by using kdestroy
        """
        clear_kerberos_tickets()
        self.cluster.shutdown()

    def refresh_kerberos_tickets(self, keytab_file, user_name, krb_conf):
        """
        Fetches a new ticket for using the keytab file and username provided.
        """
        self.ads_pid = subprocess.call(['kinit', '-t', keytab_file, user_name], env={'KRB5_CONFIG': krb_conf}, shell=False)

    def connect_and_query(self, auth_provider):
        """
        Runs a simple system query with the auth_provided specified.
        """
        os.environ['KRB5_CONFIG'] = self.krb_conf
        self.cluster = Cluster(auth_provider=auth_provider)
        self.session = self.cluster.connect()
        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        rs = self.session.execute(statement)
        return rs

    def test_should_not_authenticate_with_bad_user_ticket(self):
        """
        This tests will attempt to authenticate with a user that has a valid ticket, but is not a valid dse user.
        @since 1.0.0
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result NoHostAvailable exception should be thrown

        """
        self.refresh_kerberos_tickets(self.dseuser_keytab, "dseuser@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider)

    def test_should_not_athenticate_without_ticket(self):
        """
        This tests will attempt to authenticate with a user that is valid but has no ticket
        @since 1.0.0
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result NoHostAvailable exception should be thrown

        """
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider)

    def test_connect_with_kerberos(self):
        """
        This tests will attempt to authenticate with a user that is valid and has a ticket
        @since 1.0.0
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result Client should be able to connect and run a basic query

        """
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        rs = self.connect_and_query(auth_provider)
        self.assertIsNotNone(rs)
        connections = [c for holders in self.cluster.get_connection_holders() for c in holders.get_connections()]
        #Check to make sure our server_authenticator class is being set appropriate
        for connection in connections:
            self.assertTrue('DseAuthenticator' in connection.authenticator.server_authenticator_class)

    def test_connect_with_kerberos_and_graph(self):
        """
        This tests will attempt to authenticate with a user and execute a graph query
        @since 1.0.0
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result Client should be able to connect and run a basic graph query with authentication

        """
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)

        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        rs = self.connect_and_query(auth_provider)
        self.assertIsNotNone(rs)
        reset_graph(self.session, self._testMethodName.lower())
        self.session.default_graph_options.graph_name = self._testMethodName.lower()
        generate_classic(self.session)

        rs = self.session.execute_graph('g.V()')
        self.assertIsNotNone(rs)

    def test_connect_with_kerberos_host_not_resolved(self):
        """
        This tests will attempt to authenticate with IP, this will fail.
        @since 1.0.0
        @jira_ticket PYTHON-566
        @test_category dse auth
        @expected_result Client should error when ip is used

        """
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], resolve_host_name=False)
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider)


def clear_kerberos_tickets():
        subprocess.call(['kdestroy'], shell=False)

