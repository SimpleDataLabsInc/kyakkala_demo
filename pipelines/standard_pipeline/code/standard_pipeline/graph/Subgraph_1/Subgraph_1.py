from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, config: SubgraphConfig, in0: DataFrame) -> DataFrame:
    Config.update(config)
    df_Reformat_1 = Reformat_1(spark, in0)
    df_Repartition_1 = Repartition_1(spark, df_Reformat_1)
    df_OrderBy_1 = OrderBy_1(spark, df_Repartition_1)

    return df_OrderBy_1
