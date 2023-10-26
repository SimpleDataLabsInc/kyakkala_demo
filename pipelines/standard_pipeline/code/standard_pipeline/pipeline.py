from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from standard_pipeline.config.ConfigStore import *
from standard_pipeline.udfs.UDFs import *
from prophecy.utils import *
from standard_pipeline.graph import *

def pipeline(spark: SparkSession) -> None:
    df_customer_ds = customer_ds(spark)
    df_orders_ds = orders_ds(spark)
    df_Aggregate_1 = Aggregate_1(spark, df_orders_ds)
    df_Join_1 = Join_1(spark, df_customer_ds, df_Aggregate_1)
    df_Subgraph_1 = Subgraph_1(spark, Config.Subgraph_1, df_Join_1)
    customer_revenue(spark, df_Subgraph_1)
    df_SchemaTransform_1 = SchemaTransform_1(spark, df_customer_ds)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/standard_pipeline")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/standard_pipeline", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/standard_pipeline")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
