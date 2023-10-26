from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from customer_orders_catalog.udfs.UDFs import *

def CompareColumns_1(spark: SparkSession, in0: DataFrame, in1: DataFrame) -> DataFrame:
    from pyspark.sql.functions import lit, sum, first, explode_outer, create_map, when, coalesce, col, row_number
    from pyspark.sql.window import Window
    from functools import reduce
    valueColumnsMap = []

    for vColumn in set(in0.columns).difference({"customer_id"}):
        valueColumnsMap.extend([lit(vColumn), col(vColumn).cast("string")])

    df1 = in0.select(
        col("customer_id"), 
        explode_outer(create_map(*valueColumnsMap))\
          .alias(
          "column_name",
          "##value##"
        )
    )
    df2 = in1.select(
        col("customer_id"), 
        explode_outer(create_map(*valueColumnsMap))\
          .alias(
          "column_name",
          "##value##"
        )
    )
    df3 = in0.select(
        col("customer_id"), 
        explode_outer(create_map(*valueColumnsMap))\
          .alias(
          "column_name",
          "##value##"
        )
    )
    df4 = in1.select(
        col("customer_id"), 
        explode_outer(create_map(*valueColumnsMap))\
          .alias(
          "column_name",
          "##value##"
        )
    )

    return df1\
        .alias("exploded1")\
        .join(
          df2.alias("exploded2"),
          reduce(
            lambda a, c: a & c,
            [col("exploded1.column_name") == col("exploded2.column_name"),              col("exploded1.customer_id") == col("exploded2.customer_id")],
            lit(True)
          ),
          "full_outer"
        )\
        .select(
          coalesce(col("exploded1.column_name"), col("exploded2.column_name")).alias("column_name"), 
          coalesce(col("exploded1.customer_id"), col("exploded2.customer_id")).alias("customer_id"), 
          col(
              "exploded1.##value##"
            )\
            .alias(
            "##left_value##"
          ), 
          col(
              "exploded2.##value##"
            )\
            .alias(
            "##right_value##"
          )
        )\
        .withColumn(
          "match_count",
          when(
              coalesce(
                (
                  col("##left_value##")
                  == col(
                    "##right_value##"
                  )
                ),
                (
                  col(
                      "##left_value##"
                    )\
                    .isNull()
                  & col(
                      "##right_value##"
                    )\
                    .isNull()
                )
              ),
              lit(1)
            )\
            .otherwise(lit(0))
        )\
        .withColumn(
          "mismatch_count",
          when(
              coalesce(
                (
                  col("##left_value##")
                  != col(
                    "##right_value##"
                  )
                ),
                ~ (
                  col(
                      "##left_value##"
                    )\
                    .isNull()
                  & col(
                      "##right_value##"
                    )\
                    .isNull()
                )
              ),
              lit(1)
            )\
            .otherwise(lit(0))
        )\
        .drop(
          "##left_value##"
        )\
        .drop(
          "##right_value##"
        )\
        .withColumn("mismatch_example_left", lit(None))\
        .withColumn("mismatch_example_right", lit(None))\
        .union(
          df3\
            .alias("exploded1")\
            .join(
              df4.alias("exploded2"),
              reduce(
                lambda a, c: a & c,
                [col("exploded1.column_name") == col("exploded2.column_name"),                  col("exploded1.customer_id") == col("exploded2.customer_id")],
                lit(True)
              ),
              "full_outer"
            )\
            .select(
              coalesce(col("exploded1.column_name"), col("exploded2.column_name")).alias("column_name"), 
              coalesce(col("exploded1.customer_id"), col("exploded2.customer_id")).alias("customer_id"), 
              col(
                  "exploded1.##value##"
                )\
                .alias(
                "##left_value##"
              ), 
              col(
                  "exploded2.##value##"
                )\
                .alias(
                "##right_value##"
              )
            )\
            .withColumn(
              "match_count",
              when(
                  coalesce(
                    (
                      col("##left_value##")
                      == col(
                        "##right_value##"
                      )
                    ),
                    (
                      col(
                          "##left_value##"
                        )\
                        .isNull()
                      & col(
                          "##right_value##"
                        )\
                        .isNull()
                    )
                  ),
                  lit(1)
                )\
                .otherwise(lit(0))
            )\
            .withColumn(
              "mismatch_count",
              when(
                  coalesce(
                    (
                      col("##left_value##")
                      != col(
                        "##right_value##"
                      )
                    ),
                    ~ (
                      col(
                          "##left_value##"
                        )\
                        .isNull()
                      & col(
                          "##right_value##"
                        )\
                        .isNull()
                    )
                  ),
                  lit(1)
                )\
                .otherwise(lit(0))
            )\
            .filter(col("mismatch_count").__gt__(lit(0)))\
            .withColumn(
              "##row_number###",
              row_number().over(Window.partitionBy(col("column_name"), col("customer_id")).orderBy(col("customer_id")))
            )\
            .filter(
              (
                col("##row_number###")
                == lit(1)
              )
            )\
            .select(
              col("column_name"), 
              col("customer_id"), 
              lit(0).alias("match_count"), 
              lit(0).alias("mismatch_count"), 
              col(
                  "##left_value##"
                )\
                .alias("mismatch_example_left"), 
              col(
                  "##right_value##"
                )\
                .alias("mismatch_example_right")
            )\
            .dropDuplicates(["column_name"])
        )\
        .groupBy("column_name")\
        .agg(
          sum("match_count").alias("match_count"), 
          sum("mismatch_count").alias("mismatch_count"), 
          first(col("mismatch_example_left"), ignorenulls = True).alias("mismatch_example_left"), 
          first(col("mismatch_example_right"), ignorenulls = True).alias("mismatch_example_right"), 
          first(
              when(coalesce(col("mismatch_example_left"), col("mismatch_example_right")).isNotNull(), col("customer_id"))\
                .otherwise(lit(None)),
              ignorenulls = True
            )\
            .alias("mismatch_example_customer_id")
        )\
        .orderBy(col("mismatch_count").desc(), col("column_name"))
