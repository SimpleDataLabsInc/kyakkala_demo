from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *

def customer_account_summary(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("order_id"), 
        col("customer_id"), 
        col("amount"), 
        datediff(current_date(), col("account_open_date")).alias("account_length_days")
    )
