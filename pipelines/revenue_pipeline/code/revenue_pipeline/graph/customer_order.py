from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from revenue_pipeline.config.ConfigStore import *
from revenue_pipeline.udfs.UDFs import *

def customer_order(spark: SparkSession, in0: DataFrame):
    in0.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("overwrite")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("dbfs:/Prophecy/kyakkala@prophecy.io/customer_order.csv")
