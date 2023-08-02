from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from standard_pipeline.config.ConfigStore import *
from standard_pipeline.udfs.UDFs import *

def orders_ds(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("order_id", IntegerType(), True), StructField("customer_id", IntegerType(), True), StructField("order_status", StringType(), True), StructField("order_category", StringType(), True), StructField("order_date", DateType(), True), StructField("amount", DoubleType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/Prophecy/kyakkala@prophecy.io/OrdersDatasetInput.csv")
