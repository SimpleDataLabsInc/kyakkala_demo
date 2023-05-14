from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from .config import *
from standard_pipeline.udfs.UDFs import *

def Reformat_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("customer_id"), 
        concat(col("first_name"), lit(", "), col("last_name")).alias("full_name"), 
        concat(format_number(col("amount"), 2), lit(" $")).alias("total_expenditure"), 
        date_format(col("account_open_date"), "MMM, yyyy").alias("date_of_join")
    )
