from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_orders_catalog.config.ConfigStore import *
from customer_orders_catalog.udfs.UDFs import *

def FlattenSchema_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    flt_col = in0.withColumn("friends", explode_outer("friends")).columns
    selectCols = [col("name") if "name" in flt_col else col("name"),                   col("friends") if "friends" in flt_col else col("friends"),                   col("friends_id") if "friends_id" in flt_col else col("friends.id").alias("friends_id"),                   col("friends_name") if "friends_name" in flt_col else col("friends.name").alias("friends_name")]

    return in0.withColumn("friends", explode_outer("friends")).select(*selectCols)
