from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_orders_catalog.config.ConfigStore import *
from customer_orders_catalog.udfs.UDFs import *

def by_customerid(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.customer_id") == col("in1.customer_id")), "inner")\
        .where((col("in0.country_code") != lit("US")))\
        .select(upper(col("in0.first_name")).alias("first_name"), lower(col("in0.last_name")).alias("last_name"), col("in0.phone").alias("phone"), col("in0.email").alias("email"), col("in0.country_code").alias("country_code"), col("in0.account_open_date").alias("account_open_date"), col("in0.account_flags").alias("account_flags"), col("in1.order_id").alias("order_id"), col("in1.customer_id").alias("customer_id"), col("in1.order_status").alias("order_status"), col("in1.order_category").alias("order_category"), col("in1.order_date").alias("order_date"), col("in1.amount").alias("amount"))
