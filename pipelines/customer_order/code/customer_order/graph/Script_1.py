from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import when
    out0 = in0.withColumn(
        "amount",
        when(col("amount") < 0, raiseError("Amount cannot be negative")).otherwise(col("amount"))
    )

    return out0
