from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_orders_catalog.config.ConfigStore import *
from customer_orders_catalog.udfs.UDFs import *

def Script_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from pyspark.sql.functions import when

    if in0.filter(col("mismatch_count") > lit(10)).count() > 0:
        raise Exception("Mismatch count is greater than 10")

    out0 = in0

    return out0
