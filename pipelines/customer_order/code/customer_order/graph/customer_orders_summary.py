from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *

def customer_orders_summary(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("customer_id"))

    return df1.agg(
        count(col("order_id")).alias("number_of_orders"), 
        round(sum(col("amount")), 2).alias("total_amount"), 
        first(col("account_length_days")).alias("account_length_days")
    )
