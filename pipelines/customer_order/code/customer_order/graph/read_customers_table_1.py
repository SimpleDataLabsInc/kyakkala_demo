from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *

def read_customers_table_1(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"`{Config.db_name}`.`customers`")
