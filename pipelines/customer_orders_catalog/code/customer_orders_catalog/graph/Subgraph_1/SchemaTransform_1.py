from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from customer_orders_catalog.udfs.UDFs import *

def SchemaTransform_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "first_name",
        when(col("first_name").like("%a%"), upper(col("first_name"))).otherwise(col("first_name"))
    )
