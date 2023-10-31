from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *

def WindowFunction_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "purchase_order",
        row_number().over(Window.partitionBy(col("customer_id")).orderBy(col("order_date").asc()))
    )
