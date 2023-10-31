from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from customer_order.graph.non_us_customers_by_customer_id import *
from customer_order.config.ConfigStore import *


class non_us_customers_by_customer_idTest(BaseTestCase):

    def test_unit_test__1(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in0/schema.json',
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in0/data/test_unit_test__1.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in1/schema.json',
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in1/data/test_unit_test__1.json',
            'in1'
        )
        dfOutComputed = non_us_customers_by_customer_id(self.spark, dfIn0, dfIn1)
        assertPredicates("out", dfOutComputed, list(zip([(col("amount") > lit(0))], ["check_amount_positive"])))

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in0/schema.json',
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in0/data/test_unit_test_.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in1/schema.json',
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/in1/data/test_unit_test_.json',
            'in1'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/out/schema.json',
            'test/resources/data/customer_order/graph/non_us_customers_by_customer_id/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = non_us_customers_by_customer_id(self.spark, dfIn0, dfIn1)
        assertDFEquals(dfOut.select("phone"), dfOutComputed.select("phone"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
