from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from . import *
from .config import *

def Subgraph_1(spark: SparkSession, config: SubgraphConfig, in0: DataFrame, in00: DataFrame) -> DataFrame:
    Config.update(config)
    df_SchemaTransform_1 = SchemaTransform_1(spark, in00)
    df_CompareColumns_1 = CompareColumns_1(spark, in0, df_SchemaTransform_1)

    return df_CompareColumns_1
