from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from revenue_report.config.ConfigStore import *
from revenue_report.udfs.UDFs import *
from prophecy.utils import *
from revenue_report.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customer_ds = customer_ds(spark)
    df_orders_ds = orders_ds(spark)
    df_Join_1 = Join_1(spark, df_customer_ds, df_orders_ds)
    df_Aggregate_1 = Aggregate_1(spark, df_Join_1)
    df_Subgraph_1 = Subgraph_1(spark, Config.Subgraph_1, df_customer_ds, df_Aggregate_1)
    customer_revenue(spark, df_Subgraph_1)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/revenue_report")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/revenue_report")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
