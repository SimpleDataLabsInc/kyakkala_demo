package io.prophecy.pipelines.dxf_framework.graph

import io.prophecy.libs._
import io.prophecy.pipelines.dxf_framework.config.Context
import io.prophecy.pipelines.dxf_framework.udfs.UDFs._
import io.prophecy.pipelines.dxf_framework.udfs._
import io.prophecy.pipelines.dxf_framework.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object generate_sk_service_col {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import java.io._
    import org.apache.commons._
    import org.apache.http._
    import org.apache.http.client._
    import org.apache.http.client.methods.HttpPost
    import org.apache.http.client.entity.UrlEncodedFormEntity
    import org.apache.http.entity.StringEntity
    import java.io.IOException
    import org.apache.http.util.EntityUtils
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import spark.implicits._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    import org.apache.http.impl.client.HttpClientBuilder
    import org.apache.spark.storage.StorageLevel
    import pureconfig._
    import pureconfig.generic.auto._
    import scala.language.implicitConversions

    implicit val formats = DefaultFormats

    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

    val api_header_key =
      dbutils.secrets.get(scope = Config.api_scope, key = Config.api_key)

    case class PkSkGen(pks_sks: String, sk_value: String, p_flag: String)

    println("inside generate get_sk...") //todo
    case class SKDef(
                      tableName: String,
                      skCol: String,
                      nkCol: List[String]
                    )

    case class SKService(
                          sks: List[SKDef]
                        )

    //case class PkSkGen(pks_sks: String, sk_value: String, p_flag: String)

    def jsonStrToMap(jsonStr: String): Map[String, Any] = {
      parse(jsonStr).extract[Map[String, Any]]
    }

    def get_sk_values_response(
                                pk_to_find: List[String],
                                post_url: String
                              ): String = {
      val req =
        Map(
          "client_id" -> Config.pipeline_name,
          "request_id" -> (Config.pipeline_name + "_" + java.time.LocalDateTime.now.toString),
          "pks" -> pk_to_find,
          "detailed" -> "true"
        )
      val reqAsJson = write(req)
      // println("reqAsJson: "+reqAsJson)

      val post = new HttpPost(
        post_url
      )

      post.setHeader("Content-type", "application/json")
      post.setHeader("Ocp-Apim-Subscription-Key", api_header_key)
      post.setEntity(new StringEntity(reqAsJson, "UTF-8"))

      val response = (HttpClientBuilder.create().build()).execute(post)

      val status_code = response.getStatusLine().getStatusCode()

      if (status_code == 206){
        println("Partial results got returned. Please connect with admin and check SK service memory. It should be less than 70 percent.")
      }

      if (status_code == 429) {
        Thread.sleep(60000)
        get_sk_values_response(pk_to_find, post_url)
      }

      val entity = response.getEntity

      var response_str = EntityUtils.toString(entity, "UTF-8")
      // println("response_str get_sks : " + response_str.take(500))
      response_str
    }
    //////change..............
    // def get_sk_values(response: String, post_url:String): Map[String, scala.math.BigInt] = {
    def get_sk_values(response: String, post_url: String): List[PkSkGen] = {

      var a = ""
      var b = ""
      var c = ""
      var response_str = response
      var pk_sk_values = jsonStrToMap(response_str)
        .get("pks_sks")
        .get
        .asInstanceOf[Map[String, Any]]
        .map {
          case (k, v) => {
            // println("k: " + k)
            a = k
            b = v
              .asInstanceOf[Map[String, Any]]
              .get("value")
              .get
              .asInstanceOf[scala.math.BigInt]
              .toString
            c = v
              .asInstanceOf[Map[String, Any]]
              .get("generated")
              .get
              .asInstanceOf[Boolean]
              .toString
            PkSkGen(a, b, c)
          }
        }

      // println("pk_sk_values: " + pk_sk_values)
      pk_sk_values.toList
    }
    //////change..............

    val out0 =
      if (
        Config.enable_sk_service != "false" && spark.conf.get(
          "new_data_flag"
        ) == "true"
      ) {

        var res = in0

        // var sksConfig = ConfigSource.string(Config.optional_sk_config).loadOrThrow[SKService]

        if (Config.sk_service_col != "None") {
          /*sksConfig = SKService(
                sksConfig.sks
              )*/
          var sksConfig = List(
            SKDef(
              tableName = Config.target_table,
              skCol = Config.sk_service_col,
              nkCol = Config.primary_key.split(",").map(x => x.trim()).toList
            )
          )

          import scala.collection.parallel._
          val skServiceParallel: ParSeq[SKDef] = sksConfig.toSeq.par
          // println("skServiceParallel: " + skServiceParallel)
          val forkJoinPool =
            new java.util.concurrent.ForkJoinPool(Config.sk_service_parallel_pool)
          skServiceParallel.tasksupport = new ForkJoinTaskSupport(forkJoinPool)

          val sk_outputs = skServiceParallel.map { x =>

            // println("x.skCol:" + x.skCol)
            // println("x.nkCol:" + x.nkCol)
            println("x.tableName: " + x.tableName)
            val temp_df = res
              .where(x.skCol + " is null or " + x.skCol + " == '-1'")
              .select(
                concat_ws(
                  "~|~",
                  x.nkCol.map { x => expr(x) }: _*
                )
              )

            val post_url =
              Config.sk_service_base_url + x.tableName + "/get_sks"
            // println("post_url: " + post_url)
            var pk_to_find =
              temp_df.collect().map(x => x.getString(0)).toList.distinct
            // println("pk_to_find: " + pk_to_find.take(5))
            // println("pk_to_find_len: "+pk_to_find.length)

            val pk_batches =
              pk_to_find.toSet.grouped(Config.sk_service_batch_size).toList

            import scala.collection.mutable.ListBuffer

            var temp_list: ListBuffer[PkSkGen] = ListBuffer()
            // l +=  "rishabh"
            var while_count = 0
            pk_batches.foreach { x =>
              val sk_input = x.toList
              var sk_response_output = (get_sk_values(
                get_sk_values_response(sk_input, post_url),
                post_url
              ))

              while (
                sk_response_output.length != sk_input.length && while_count < 5
              ) {
                println("sk_response_output length: " + sk_response_output.length)
                println("sk_input length: " + sk_input.length)
                Thread.sleep(61000)
                while_count = while_count + 1
                println("while_count: " + while_count)
                sk_response_output = (get_sk_values(
                  get_sk_values_response(sk_input, post_url),
                  post_url
                ))
                if (while_count == 3) {
                  throw new Exception(
                    "Please check SK service. Input response size and output response size did not matched in the response after 3 retries with 60 sec gap. Please connect with admin and see memory usaage for redis cache and clear it in case it is more that 70 percent."
                  )
                }

              }
              temp_list = temp_list ++ sk_response_output

            }
            // println("temp_list: " + temp_list)
            val sk_seq = temp_list
              .map(row_val => (row_val.pks_sks, row_val.sk_value, row_val.p_flag))
              .toSeq
            // println("sk_seq: " + sk_seq)
            sk_seq
          }

          sksConfig.zip(sk_outputs).foreach {
            case (x, y) => // sksConfig = optional sk config
              if (true) {
                println("inside get sk join loop")
                val helper_col_pf = x.tableName + "_pf_" + x.skCol
                val helper_col_sk = x.tableName + "_sk_" + x.skCol
                val helper_col_nk = x.tableName + "_nk_" + x.skCol
                res = res
                  .withColumn(
                    x.tableName + "_concat_pk",
                    concat_ws(
                      "~|~",
                      x.nkCol.map { nk_col => expr(nk_col) }: _*
                    )
                  )
                  .as("in0")
                  .join(
                    y.toDF("concat_pk", "service_sk", "placeholder_flag").as("in1"),
                    expr("in0." + x.tableName + "_concat_pk == in1.concat_pk"),
                    "left"
                  )
                  .withColumn(
                    x.skCol,
                    when(
                      (col(x.skCol) === lit("-1")) || col(x.skCol).isNull,
                      col("service_sk")
                    ).otherwise(col(x.skCol)).cast("long")
                  )
                  .withColumn(
                    helper_col_pf,
                    col("in1.placeholder_flag")
                  )
                  .withColumn(helper_col_sk, col("in1.service_sk"))
                  .withColumn(helper_col_nk, col("in1.concat_pk"))
                res = res
                  .selectExpr(
                    "in0.*",
                    x.skCol,
                    helper_col_pf,
                    helper_col_sk,
                    helper_col_nk
                  )
              }
          }
        }
        res
      } else {
        in0
      }
    out0
  }

}
