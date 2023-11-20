package samples


import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

case class COnfig(api_scope: String, api_key: String, api_secret: String, pipeline_name: String, endpoint: String, enable_sk_service: String, sk_service_col: String, target_table: String, primary_key: String, sk_service_parallel_pool: Int, sk_service_base_url: String, sk_service_batch_size: Int)

object generate_sk_service_col {
  def apply(spark: SparkSession, Config: COnfig, in0: DataFrame): DataFrame = {
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

    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

    val api_header_key =
      dbutils.secrets.get(scope = Config.api_scope, key = Config.api_key)

    // case class PkSkGen(pks_sks: String, sk_value: String, p_flag: String)

    println("inside generate get_sk...") //todo
    case class SKDef(tableName: String, skCol: String, nkCol: List[String])
    case class SKService(sks: List[SKDef])


    def jsonStrToMap(jsonStr: String): Map[String, Any] = {
      implicit val formats = DefaultFormats
      parse(jsonStr).extract[Map[String, Any]]
    }

    def get_sk_values_response(
                                pipeline_name: String,
                                pk_to_find: List[String],
                                post_url: String
                              ): String = {

      implicit val formats = DefaultFormats
      val req =
        Map(
          "client_id" -> pipeline_name,
          "request_id" -> (pipeline_name + "_" + java.time.LocalDateTime.now.toString),
          "pks" -> pk_to_find,
          "detailed" -> "true"
        )
      val reqAsJson = write(req)
      // println("reqAsJson: "+reqAsJson)

      val post = new HttpPost(post_url)

      post.setHeader("Content-type", "application/json")
      post.setHeader("Ocp-Apim-Subscription-Key", api_header_key)
      post.setEntity(new StringEntity(reqAsJson, "UTF-8"))

      val response = (HttpClientBuilder.create().build()).execute(post)
      val status_code = response.getStatusLine().getStatusCode()

      if (status_code == 206) {
        println("Partial results got returned. Please connect with admin and check SK service memory. It should be less than 70 percent.")
      } else if (status_code == 429) {
        Thread.sleep(60000)
        get_sk_values_response(pipeline_name, pk_to_find, post_url)
      }

      val entity = response.getEntity
      val response_str = EntityUtils.toString(entity, "UTF-8")
      // println("response_str get_sks : " + response_str.take(500))
      response_str
    }

    def get_sk_values(response: String, post_url: String): List[(String, String, String)] = {
      var a = ""
      var b = ""
      var c = ""
      val pk_sk_values = jsonStrToMap(response)
        .get("pks_sks").get.asInstanceOf[Map[String, Any]]
        .map {
          case (k, v) =>
            // println("k: " + k)
            a = k
            b = v.asInstanceOf[Map[String, Any]].get("value").get.asInstanceOf[scala.math.BigInt].toString
            c = v.asInstanceOf[Map[String, Any]].get("generated").get.asInstanceOf[Boolean].toString
            (a, b, c)
        }
      // println("pk_sk_values: " + pk_sk_values)
      pk_sk_values.toList
    }

    val out0 =
      if (Config.enable_sk_service != "false" && spark.conf.get("new_data_flag") == "true" && Config.sk_service_col != "None") {
        val sksConfig = SKDef(
          tableName = Config.target_table,
          skCol = Config.sk_service_col,
          nkCol = Config.primary_key.split(",").map(x => x.trim()).toList
        )

        println("sksConfig.tableName: " + sksConfig.tableName)
        val temp_df = in0
          .where(sksConfig.skCol + " is null or " + sksConfig.skCol + " == '-1'")
          .select(
            concat_ws(
              "~|~",
              sksConfig.nkCol.map { x => expr(x) }: _*
            )
          ).dropDuplicates()

        val post_url = Config.sk_service_base_url + sksConfig.tableName + "/get_sks"
        val sk_service_batch_size = Config.sk_service_batch_size
        val pipeline_name = Config.pipeline_name
        val partitionCount = temp_df.rdd.getNumPartitions

        print(s"Generating sks with $partitionCount partitions for ${temp_df.count}")
        val sk_outputs_parts = (0 until partitionCount).map { i =>
          temp_df.rdd.mapPartitionsWithIndex { (idx, partitionRows) =>
            if (i == idx) {
              var while_count = 0
              partitionRows.grouped(sk_service_batch_size)
                .flatMap { rowGroup =>
                  val sk_input = rowGroup.map(_.getString(0)).toList
                  var sk_response_output = get_sk_values(
                    get_sk_values_response(pipeline_name, sk_input, post_url),
                    post_url
                  )
                  while (sk_response_output.length != sk_input.length && while_count < 5) {
                    println("sk_response_output length: " + sk_response_output.length)
                    println("sk_input length: " + sk_input.length)
                    Thread.sleep(61000)
                    while_count = while_count + 1
                    println("while_count: " + while_count)
                    sk_response_output = get_sk_values(
                      get_sk_values_response(pipeline_name, sk_input, post_url),
                      post_url
                    )
                    if (while_count == 3) {
                      throw new Exception(
                        "Please check SK service. Input response size and output response size did not matched in the response after 3 retries with 60 sec gap. Please connect with admin and see memory usaage for redis cache and clear it in case it is more that 70 percent."
                      )
                    }
                  }
                  sk_response_output
                }
            } else Iterator[(String, String, String)]()
          }
        }

        val sk_outputs = sk_outputs_parts.reduceLeft(_ union _).toDF("concat_pk", "service_sk", "placeholder_flag")

        val helper_col_pf = sksConfig.tableName + "_pf_" + sksConfig.skCol
        val helper_col_sk = sksConfig.tableName + "_sk_" + sksConfig.skCol
        val helper_col_nk = sksConfig.tableName + "_nk_" + sksConfig.skCol
        in0.withColumn(
          sksConfig.tableName + "_concat_pk",
          concat_ws("~|~", sksConfig.nkCol.map { nk_col => expr(nk_col) }: _*)
        ).as("in0")
          .join(
            sk_outputs.as("in1"),
            expr("in0." + sksConfig.tableName + "_concat_pk == in1.concat_pk"),
            "left"
          )
          .withColumn(
            sksConfig.skCol,
            when(
              col(sksConfig.skCol) === lit("-1") || col(sksConfig.skCol).isNull,
              col("service_sk")
            ).otherwise(col(sksConfig.skCol)).cast("long")
          )
          .withColumn(helper_col_pf, col("in1.placeholder_flag"))
          .withColumn(helper_col_sk, col("in1.service_sk"))
          .withColumn(helper_col_nk, col("in1.concat_pk"))
          .selectExpr("in0.*", sksConfig.skCol, helper_col_pf, helper_col_sk, helper_col_nk)
      } else {
        in0
      }
    out0
  }

}
