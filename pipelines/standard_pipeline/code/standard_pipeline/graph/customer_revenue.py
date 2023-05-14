from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from standard_pipeline.config.ConfigStore import *
from standard_pipeline.udfs.UDFs import *

def customer_revenue(spark: SparkSession, in0: DataFrame):
    in0.write.format("json").mode("overwrite").save("dbfs:/Prophecy/kyakkala@prophecy.io/cust_ord.csv/")
