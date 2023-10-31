import unittest

from test.customer_order.graph.test_non_us_customers_by_customer_id import *

if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(unittest.TestSuite())
