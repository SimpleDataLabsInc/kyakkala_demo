from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from customer_orders_catalog.config.ConfigStore import *
from customer_orders_catalog.udfs.UDFs import *

def customer_nested_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("json")\
        .option("multiLine", True)\
        .schema(
          StructType([
            StructField("_id", StringType(), True), StructField("about", StringType(), True), StructField("address", StringType(), True), StructField("age", LongType(), True), StructField("balance", StringType(), True), StructField("company", StringType(), True), StructField("email", StringType(), True), StructField("eyeColor", StringType(), True), StructField("favoriteFruit", StringType(), True), StructField("friends", ArrayType(StructType([StructField("id", LongType(), True), StructField("name", StringType(), True)]), True), True), StructField("gender", StringType(), True), StructField("greeting", StringType(), True), StructField("guid", StringType(), True), StructField("index", LongType(), True), StructField("isActive", BooleanType(), True), StructField("latitude", DoubleType(), True), StructField("longitude", DoubleType(), True), StructField("name", StringType(), True), StructField("phone", StringType(), True), StructField("picture", StringType(), True), StructField("registered", StringType(), True), StructField("tags", ArrayType(StringType(), True), True)
        ])
        )\
        .load("dbfs:/FileStore/kyakkala/employee_details.json")
