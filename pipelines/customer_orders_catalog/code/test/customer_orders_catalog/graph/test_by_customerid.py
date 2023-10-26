from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from customer_orders_catalog.graph.by_customerid import *
from customer_orders_catalog.config.ConfigStore import *


class by_customeridTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_orders_catalog/graph/by_customerid/in0/schema.json',
            'test/resources/data/customer_orders_catalog/graph/by_customerid/in0/data/test_unit_test_.json',
            'in0'
        )
        dfIn1 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/customer_orders_catalog/graph/by_customerid/in1/schema.json',
            'test/resources/data/customer_orders_catalog/graph/by_customerid/in1/data/test_unit_test_.json',
            'in1'
        )
        dfOutComputed = by_customerid(self.spark, dfIn0, dfIn1)
        assertPredicates("out", dfOutComputed, list(zip([(col("order_id") > lit(0))], ["Check order_id"])))

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
