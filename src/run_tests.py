import unittest

from tests.test_csv import TestDAQJobStoreCSV


def run_tests():
    test_suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    test_suite.addTests(loader.loadTestsFromTestCase(TestDAQJobStoreCSV))
    return test_suite


if __name__ == "__main__":
    test_suite = run_tests()
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
