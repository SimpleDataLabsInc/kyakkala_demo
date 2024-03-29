from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from standard_pipeline.config.ConfigStore import *
from standard_pipeline.udfs.UDFs import *

def Aggregate_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("customer_id"))

    return df1.agg(sum(col("amount")).alias("amount"))
