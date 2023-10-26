from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *
from prophecy.utils import *
from customer_order.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customers = customers(spark)
    df_orders = orders(spark)
    df_by_customerid = by_customerid(spark, df_customers, df_orders)
    df_Cleanup = Cleanup(spark, df_by_customerid)
    df_Aggregate_1_1 = Aggregate_1_1(spark, df_Cleanup)
    customer_orders_catalog(spark, df_Aggregate_1_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customer_order")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/customer_order", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/customer_order")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
