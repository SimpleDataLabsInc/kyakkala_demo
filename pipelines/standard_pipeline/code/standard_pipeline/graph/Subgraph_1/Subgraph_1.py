from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, config: SubgraphConfig, in0: DataFrame, in1: DataFrame) -> DataFrame:
    Config.update(config)
    df_Join_2 = Join_2(spark, in0, in1)
    df_Reformat_1 = Reformat_1(spark, df_Join_2)
    df_OrderBy_1 = OrderBy_1(spark, df_Reformat_1)
    df_Repartition_1 = Repartition_1(spark, df_OrderBy_1)

    return df_Repartition_1
