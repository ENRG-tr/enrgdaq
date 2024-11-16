import unittest

if __name__ == "__main__":
    loader = unittest.TestLoader()
    import os

    suite = loader.discover(
        os.path.join(os.path.dirname(__file__), "tests"), pattern="test_*.py"
    )
    runner = unittest.TextTestRunner()
    runner.run(suite)
