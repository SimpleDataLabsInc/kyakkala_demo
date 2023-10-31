from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from customer_order.config.ConfigStore import *
from customer_order.udfs.UDFs import *
from prophecy.utils import *
from customer_order.graph import *

def pipeline(spark: SparkSession) -> None:
    df_read_customers_table_1 = read_customers_table_1(spark)
    df_SQLStatement_1 = SQLStatement_1(spark, df_read_customers_table_1)
    df_read_customers_table = read_customers_table(spark)
    df_read_orders_table = read_orders_table(spark)
    df_non_us_customers_by_customer_id = non_us_customers_by_customer_id(
        spark, 
        df_read_customers_table, 
        df_read_orders_table
    )

    if (Config.enable_customer_order_laod == true):
        df_customer_account_summary = customer_account_summary(spark, df_non_us_customers_by_customer_id)
    else:
        df_customer_account_summary = df_non_us_customers_by_customer_id

    df_customer_orders_summary = customer_orders_summary(spark, df_customer_account_summary)
    df_Subgraph_1 = Subgraph_1(
        spark, 
        Config.Subgraph_1, 
        df_non_us_customers_by_customer_id, 
        df_non_us_customers_by_customer_id
    )
    df_WindowFunction_1 = WindowFunction_1(spark, df_non_us_customers_by_customer_id)
    customer_orders_catalog(spark, df_customer_orders_summary)
    df_Script_1 = Script_1(spark, df_non_us_customers_by_customer_id)

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
