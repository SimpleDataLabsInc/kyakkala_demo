from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from standard_pipeline.config.ConfigStore import *
from standard_pipeline.udfs.UDFs import *

def Join_1(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.customer_id") == col("in1.customer_id")), "inner")\
        .select(col("in0.customer_id").alias("customer_id"), col("in1.amount").alias("amount"), kk_square(col("amount")).alias("s_amount"))
