import unittest

from tests.test_csv import TestDAQJobStoreCSV
from tests.test_handle_alerts import TestDAQJobHandleAlerts
from tests.test_handle_stats import TestDAQJobHandleStats
from tests.test_healthcheck import TestDAQJobHealthcheck
from tests.test_main import TestMain
from tests.test_n1081b import TestDAQJobN1081B
from tests.test_slack import TestDAQJobAlertSlack


def run_tests():
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobStoreCSV))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobN1081B))
    test_suite.addTests(loader.loadTestsFromTestCase(TestMain))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobHandleStats))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobAlertSlack))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobHealthcheck))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobHandleAlerts))
    return test_suite


if __name__ == "__main__":
    test_suite = run_tests()
    runner = unittest.TextTestRunner(verbosity=1)
    runner.run(test_suite)
