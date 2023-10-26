from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_orders_catalog.config.ConfigStore import *
from customer_orders_catalog.udfs.UDFs import *

def customer_orders_catalog(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("`prophecy_demos`.`customer_orders`")
