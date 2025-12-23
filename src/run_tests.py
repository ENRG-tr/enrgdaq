import logging
import os
import sys
import unittest

import coverage

from enrgdaq.utils.test import set_is_unit_testing

if __name__ == "__main__":
    set_is_unit_testing(True)

    logging.disable(logging.CRITICAL)

    loader = unittest.TestLoader()
    cov = coverage.Coverage(omit=["*/config-3.py", "*/config.py"])
    cov.start()
    suite = loader.discover(
        os.path.join(os.path.dirname(__file__), "tests"), pattern="test_*.py"
    )

    original_stderr = sys.stderr
    sys.stderr = open(os.devnull, "w")

    runner = unittest.TextTestRunner(stream=original_stderr, verbosity=2)
    result = runner.run(suite)

    sys.stderr.close()
    sys.stderr = original_stderr

    cov.stop()
    cov.save()

    # cov.report()
