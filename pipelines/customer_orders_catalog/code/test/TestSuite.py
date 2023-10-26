import unittest

from test.customer_orders_catalog.graph.test_by_customerid import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
