import sys
import unittest

from tests.test_csv import TestDAQJobStoreCSV
from tests.test_handle_alerts import TestDAQJobHandleAlerts
from tests.test_handle_stats import TestDAQJobHandleStats
from tests.test_healthcheck import TestDAQJobHealthcheck
from tests.test_n1081b import TestDAQJobN1081B
from tests.test_remote import TestDAQJobRemote
from tests.test_slack import TestDAQJobAlertSlack
from tests.test_supervisor import TestSupervisor


def run_tests():
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobStoreCSV))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobN1081B))
    test_suite.addTests(loader.loadTestsFromTestCase(TestSupervisor))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobHandleStats))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobAlertSlack))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobHealthcheck))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobHandleAlerts))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobRemote))
    return test_suite


if __name__ == "__main__":
    test_suite = run_tests()
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(test_suite)

    sys.exit(not result.wasSuccessful())
