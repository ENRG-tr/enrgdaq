import unittest

from tests.test_csv import TestDAQJobStoreCSV
from tests.test_n1081b import TestDAQJobN1081B


def run_tests():
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobStoreCSV))
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobN1081B))
    return test_suite


if __name__ == "__main__":
    test_suite = run_tests()
    runner = unittest.TextTestRunner()
    runner.run(test_suite)