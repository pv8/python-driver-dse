# Copyright 2016 DataStax, Inc.
import logging

from dse.graph import (SimpleGraphStatement)
from tests.integration import BasicGraphUnitTestCase, use_singledc_wth_graph_and_spark, generate_classic, find_spark_master
log = logging.getLogger(__name__)


def setup_module():
    use_singledc_wth_graph_and_spark()


class SparkLBTests(BasicGraphUnitTestCase):
    """
    Test to validate that analtics query can run in a multi-node enviroment. Also check to to ensure
    that the master spark node is correctly targeted when OLAP queries are run

    @since 1.0.0
    @jira_ticket PYTHON-510
    @expected_result OLAP results should come back correctly, master spark coordinator should always be picked.
    @test_category dse graph
    """
    def test_spark_analytic_query(self):
        generate_classic(self.session)
        spark_master = find_spark_master(self.session)

        # Run multipltle times to ensure we don't round robin
        for i in range(3):
            to_run = SimpleGraphStatement("g.V().count()")
            to_run.options.set_source_analytics()
            rs = self.session.execute_graph(to_run)
            self.assertEqual(rs[0].value, 6)
            self.assertEqual(rs.response_future._current_host.address, spark_master)
