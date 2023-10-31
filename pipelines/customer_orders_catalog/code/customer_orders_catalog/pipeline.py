from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from customer_orders_catalog.config.ConfigStore import *
from customer_orders_catalog.udfs.UDFs import *
from prophecy.utils import *
from customer_orders_catalog.graph import *

def pipeline(spark: SparkSession) -> None:
    df_raw_customer_ds = raw_customer_ds(spark)
    df_raw_orders_ds = raw_orders_ds(spark)
    orders_delta(spark, df_raw_orders_ds)
    df_customers = customers(spark)
    df_orders_catalog = orders_catalog(spark)
    df_by_customerid = by_customerid(spark, df_customers, df_orders_catalog)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_by_customerid)
    df_CompareColumns_1 = CompareColumns_1(spark, df_by_customerid, df_SchemaTransform_1)
    df_customer_nested_data = customer_nested_data(spark)
    df_WindowFunction_1 = WindowFunction_1(spark, df_by_customerid)
    df_Cleanup = Cleanup(spark, df_by_customerid)
    df_Aggregate_1_1 = Aggregate_1_1(spark, df_Cleanup)
    df_FlattenSchema_1 = FlattenSchema_1(spark, df_customer_nested_data)
    df_Script_1 = Script_1(spark, df_CompareColumns_1)
    customer_delta(spark, df_raw_customer_ds)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/customer_orders_catalog")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/customer_orders_catalog", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/customer_orders_catalog")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
