import os
import unittest

import coverage

if __name__ == "__main__":
    loader = unittest.TestLoader()
    cov = coverage.Coverage(omit=["*/config-3.py", "*/config.py"])
    cov.start()
    suite = loader.discover(
        os.path.join(os.path.dirname(__file__), "tests"), pattern="test_*.py"
    )
    runner = unittest.TextTestRunner()
    # Set ENRGDAQ_IS_UNIT_TESTING to True
    os.environ["ENRGDAQ_IS_UNIT_TESTING"] = "True"
    runner.verbosity = 2
    runner.run(suite)

    cov.stop()
    cov.save()

    # cov.report()
