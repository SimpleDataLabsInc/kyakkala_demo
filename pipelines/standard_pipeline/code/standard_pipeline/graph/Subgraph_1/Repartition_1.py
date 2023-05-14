from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from .config import *
from standard_pipeline.udfs.UDFs import *

def Repartition_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.coalesce(1)
