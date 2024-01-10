import pathlib
import sys
import unittest

suite = unittest.TestLoader().discover(pathlib.Path(__file__).parent.resolve())
result = unittest.TextTestRunner(verbosity=2).run(suite)

sys.exit(not result.wasSuccessful())
