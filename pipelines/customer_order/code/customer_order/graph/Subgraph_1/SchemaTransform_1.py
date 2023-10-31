from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from customer_order.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "last_name",
        when(col("last_name").like("%a%"), upper(col("last_name"))).otherwise(col("last_name"))
    )
