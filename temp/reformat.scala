package io.prophecy.pipelines.dxf_framework.graph

import io.prophecy.libs._
import io.prophecy.pipelines.dxf_framework.config.Context
import io.prophecy.pipelines.dxf_framework.udfs.UDFs._
import io.prophecy.pipelines.dxf_framework.udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object apply_reformat_rules {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    import _root_.io.prophecy.libs._

    println("#########################################")
    println("#####Step name: apply_reformat_rules#####")
    println("#########################################")
    println(
      "step start time: " + Instant.now().atZone(ZoneId.of("America/Chicago"))
    )
    // parse config json of key-value pair where key is target column name and value is expression

    val out0 =
      if (
        Config.reformat_rules != "None" && spark.conf.get("new_data_flag") == "true"
      ) {
        import org.json4s._
        import org.json4s.jackson.JsonMethods._
        import scala.collection.mutable.ListBuffer
        import java.text.SimpleDateFormat
        import org.apache.spark.storage.StorageLevel

        registerAllUDFs(spark: SparkSession)
        // val sv_apply = udf((input1: Any, input2: String, input3: String) ⇒ null)
        //   .withName("sv_apply")
        // val sv_create_collection = udf((input1: String, input2: String) ⇒ null)
        //   .withName("sv_create_collection")
        // val Get_full_day_of_week_from_number =
        //   udf(
        //     (input: Int) ⇒
        //       input match {
        //         case 1 ⇒ "SUNDAY"
        //         case 2 ⇒ "MONDAY"
        //         case 3 ⇒ "TUESDAY"
        //         case 4 ⇒ "WEDNESDAY"
        //         case 5 ⇒ "THURSDAY"
        //         case 6 ⇒ "FRIDAY"
        //         case 7 ⇒ "SATURDAY"
        //         case _ ⇒ "SATURDAY"
        //       },
        //     StringType
        //   )
        //     .withName("Get_full_day_of_week_from_number")

        // val Get_Clean_Date_Local_Udf =
        //   udf(
        //     (
        //         Clean_Date: String,
        //         Loc_nm: String,
        //         Ship_Date: String,
        //         v_Day_of_Week: Int,
        //         v_Holiday_Ind: String,
        //         v_Nxt_BusDay_StartTime: String
        //     ) ⇒
        //       if (Clean_Date == null) "1900-01-01 00:00:000"
        //       else if (
        //         Clean_Date != null && ((Loc_nm != null && Loc_nm == "-") || Loc_nm == null)
        //       ) Clean_Date
        //       else if (
        //         Ship_Date != null && (Clean_Date.take(10) == Ship_Date
        //           .take(10)) && v_Day_of_Week == 7
        //       ) Clean_Date
        //       else if (
        //         Set(2, 3, 4, 5, 6).contains(v_Day_of_Week) && v_Holiday_Ind == "N"
        //       ) Clean_Date
        //       else if (
        //         Set(2, 3, 4, 5, 6).contains(v_Day_of_Week) && v_Holiday_Ind == "Y"
        //       ) v_Nxt_BusDay_StartTime
        //       else if (Set(1, 7).contains(v_Day_of_Week)) v_Nxt_BusDay_StartTime
        //       else null,
        //     StringType
        //   )
        //     .withName("Get_Clean_Date_Local_Udf")

        // val Get_Ship_Date_Local_Udf =
        //   udf(
        //     (
        //         Ship_Date: String,
        //         Loc_nm: String,
        //         Clean_Date: String,
        //         v_Day_of_Week: String,
        //         v_Nxt_BusDay_StartTime: String,
        //         v_sd_sundaygt_strtime_flag: String,
        //         v_Ship_Date_With_Ex_Hr: String,
        //         v_Ship_Date_With_Ex_Day: String,
        //         v_Ship_Date_With_Sat_Ex_Hr: String,
        //         ship_date_strip_hh: String,
        //         ship_date_strip_mi: String
        //     ) ⇒
        //       if (Ship_Date == null) "1900-01-01 00:00:00"
        //       else if (
        //         Ship_Date != null && ((Loc_nm != null && Loc_nm == "-") || Loc_nm == null)
        //       ) Ship_Date
        //       else if (
        //         Clean_Date != null && (Clean_Date.take(10) == Ship_Date
        //           .take(10)) && v_Day_of_Week.toInt == 7
        //       ) Ship_Date
        //       else if (Set(2, 3, 4, 5, 6).contains(v_Day_of_Week.toInt)) Ship_Date
        //       else if (
        //         v_Day_of_Week.toInt == 7 && (((ship_date_strip_hh.toInt * 60) + ship_date_strip_mi.toInt) > (if (
        //                                                                                                        v_Nxt_BusDay_StartTime == null
        //                                                                                                      )
        //                                                                                                        0
        //                                                                                                      else
        //                                                                                                        v_Nxt_BusDay_StartTime.toInt))
        //       ) v_Ship_Date_With_Sat_Ex_Hr
        //       else if (
        //         v_Day_of_Week.toInt == 1 && v_sd_sundaygt_strtime_flag != null && v_sd_sundaygt_strtime_flag == "Y"
        //       )
        //         v_Ship_Date_With_Ex_Hr
        //       else if (
        //         v_Day_of_Week.toInt == 1 && v_sd_sundaygt_strtime_flag != null && v_sd_sundaygt_strtime_flag == "N"
        //       )
        //         v_Ship_Date_With_Ex_Day
        //       else
        //         Ship_Date,
        //     StringType
        //   )
        //     .withName("Get_Ship_Date_Local_Udf")

        // val Weekend_Day_Cnt_Inner =
        //   udf(
        //     {
        //       (
        //           v_days_diff: Int,
        //           v_Day_of_Week: Int,
        //           v_remaining_days: Int
        //       ) ⇒
        //         var v_no_of_weekend: Double = 0
        //         if (
        //           v_days_diff != null && ((v_days_diff >= 7 && Set(2, 3, 4, 5, 6)
        //             .contains(v_Day_of_Week)) || (v_days_diff > 7 && Set(1, 7)
        //             .contains(v_Day_of_Week)))
        //         )
        //           v_no_of_weekend = v_days_diff / 7
        //         else if (
        //           v_days_diff != null && v_days_diff == 7 && Set(1, 7)
        //             .contains(v_Day_of_Week)
        //         )
        //           v_no_of_weekend = 0.5
        //         if (
        //           v_Day_of_Week != null && ((v_Day_of_Week == 6 && v_remaining_days > 2) || (v_Day_of_Week == 5 && v_remaining_days > 3) || (v_Day_of_Week == 4 && v_remaining_days > 4) || (v_Day_of_Week == 3 && v_remaining_days > 5) || (v_Day_of_Week == 2 && v_remaining_days > 6))
        //         )
        //           v_no_of_weekend = v_no_of_weekend + 1;
        //         else if (
        //           v_Day_of_Week != null && (v_Day_of_Week == 6 && v_remaining_days == 2) || (v_Day_of_Week == 5 && v_remaining_days == 3) || (v_Day_of_Week == 4 && v_remaining_days == 4) || (v_Day_of_Week == 3 && v_remaining_days == 5) || (v_Day_of_Week == 2 && v_remaining_days == 6) || (v_Day_of_Week == 7 && v_remaining_days > 1)
        //         )
        //           v_no_of_weekend = v_no_of_weekend + 0.5;
        //         v_no_of_weekend
        //     },
        //     DoubleType
        //   )
        //     .withName("Weekend_Day_Cnt_Inner")
        // val Get_V_Final_Days_To_Ship_Udf =
        //   udf(
        //     (
        //         v_days_to_ship: Double,
        //         v_Holiday_Ind: String,
        //         v_Day_of_Week: Int
        //     ) ⇒
        //       if (v_days_to_ship <= 0) 0
        //       else if ((v_Holiday_Ind == "Y") || Set(1, 7).contains(v_Day_of_Week))
        //         v_days_to_ship - 1
        //       else v_days_to_ship,
        //     DoubleType
        //   )
        //     .withName("Get_V_Final_Days_To_Ship_Udf")

        // val Get_V_Final_Tat_Hours_Udf =
        //   udf(
        //     (
        //         v_f_tat_hours: Double,
        //         loc_nm: String
        //     ) ⇒ if (v_f_tat_hours < 0 || loc_nm == "-") 0 else v_f_tat_hours,
        //     DoubleType
        //   )
        //     .withName("Get_V_Final_Tat_Hours_Udf")

        // val Calculate_TAT_Hours_Udf =
        //   udf(
        //     {
        //       (
        //           Clean_Date: String,
        //           Ship_Date: String
        //       ) ⇒
        //         var v_tat_hours: Double = 0.0
        //         if (
        //           Clean_Date == null || Ship_Date == null || Clean_Date > Ship_Date || Clean_Date == "1900-01-01 00:00:00" || Ship_Date == "1900-01-01 00:00:00"
        //         )
        //           v_tat_hours = 0
        //         else {
        //           val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //             .parse(Ship_Date)
        //             .getTime
        //           val startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //             .parse(Clean_Date)
        //             .getTime
        //           v_tat_hours = (endTime - startTime) / 3600000.0
        //         }
        //     },
        //     DoubleType
        //   )
        //     .withName("Calculate_TAT_Hours_Udf")

        // val extract_mel_dates_Udf =
        //   udf(
        //     {
        //       (
        //           in_mpa_thru_dt: String,
        //           lkp_member_elig_rec_vec: Seq[Row]
        //       ) ⇒
        //         var j = 0
        //         val member_elig_rec_vec = ListBuffer[Row]()
        //         while (j < lkp_member_elig_rec_vec.length) {
        //           val lkp_member_elig_rec = lkp_member_elig_rec_vec(j)
        //           val lkp_member_elig_rec_mel_stat_cd =
        //             lkp_member_elig_rec.getAs[String]("mel_stat_cd")
        //           val lkp_member_elig_rec_mel_eff_dt =
        //             lkp_member_elig_rec.getAs[String]("mel_eff_dt")
        //           val lkp_member_elig_rec_mel_thru_dt =
        //             lkp_member_elig_rec.getAs[String]("mel_thru_dt")
        //           val lkp_member_elig_rec_seq_nbr =
        //             lkp_member_elig_rec.getAs[String]("seq_nbr")
        //           if (
        //             lkp_member_elig_rec_mel_stat_cd == "A" && in_mpa_thru_dt > lkp_member_elig_rec_mel_eff_dt
        //           ) {
        //             member_elig_rec_vec += Row(
        //               _first_defined(
        //                 lkp_member_elig_rec_mel_eff_dt,
        //                 "1900-01-01"
        //               ).toString,
        //               _first_defined(
        //                 lkp_member_elig_rec_mel_thru_dt,
        //                 "9999-12-31"
        //               ).toString,
        //               lkp_member_elig_rec_seq_nbr
        //             )
        //           }
        //           j += 1
        //         }
        //         if (member_elig_rec_vec.size == 0) {
        //           member_elig_rec_vec += Row("1900-01-01", "9999-12-31", "0")
        //         }
        //         member_elig_rec_vec.sortBy(_.toSeq.last.toString).head
        //     },
        //     StructType(
        //       List(
        //         StructField("mel_eff_dt", StringType, true),
        //         StructField("mel_thru_dt", StringType, true),
        //         StructField("seq_nbr", StringType, true)
        //       )
        //     )
        //   )
        //     .withName("extract_mel_dates_Udf")

        // val get_partial_drug_details_Udf =
        //   udf(
        //     {
        //       (
        //           v_prod_id: String,
        //           v_gpi_no: String,
        //           v_filled_dt: String,
        //           v_mel_thrgh_dt: String,
        //           v_mpa_thrgh_dt: String,
        //           lkp_cag_ndc_gpi: Seq[Row],
        //           lkp_ndc_gpi_list: Seq[Seq[Row]]
        //       ) ⇒
        //         var exact_inc_flg: Int = 0
        //         var exact_exc_flg: Int = 0
        //         val c_valid_rec: Int = 0
        //         var m_valid_rec: Int = 0
        //         var v_prior_auth_flg: Int = 0

        //         var rule_cnt = 0
        //         val count = lkp_cag_ndc_gpi.size

        //         while (count > rule_cnt) {
        //           val v_rec = lkp_cag_ndc_gpi(rule_cnt)

        //           val v_rec_drug_name_list = _first_defined(
        //             v_rec.getAs[String]("drug_name_list"),
        //             '-'
        //           ).toString
        //           val v_rec_drug_type = v_rec.getAs[String]("drug_type")
        //           val v_rec_lookback_days = v_rec.getAs[String]("lookback_days")
        //           val v_rec_mel_lookforward = v_rec.getAs[String]("mel_lookforward")
        //           val v_rec_include_exclude = v_rec.getAs[String]("include_exclude")
        //           val v_rec_pa_lookforward = v_rec.getAs[String]("pa_lookforward")
        //           var c_valid_rec = 0

        //           if (
        //             List("GPI LIST", "NDC LIST").contains(
        //               _string_upcase(v_rec_drug_type)
        //             ) && v_rec_drug_name_list != null
        //           ) {
        //             val ndc_count = lkp_ndc_gpi_list(rule_cnt).size
        //             var ndc_rule_cnt = 0;
        //             while (ndc_count > ndc_rule_cnt) {
        //               val v_ndc_rec = lkp_ndc_gpi_list(rule_cnt)(ndc_count)
        //               val v_ndc_rec_ndc_gpi = v_ndc_rec.getAs[String]("ndc_gpi")
        //               val v_ndc_rec_list_name = v_ndc_rec.getAs[String]("list_name")

        //               val drug_match =
        //                 if (
        //                   _isnull(v_rec_drug_type) || _is_blank(
        //                     v_rec_drug_type
        //                   ) || _first_defined(
        //                     v_rec_drug_type,
        //                     '-'
        //                   ) == '-' || (_string_upcase(
        //                     v_rec_drug_type
        //                   ) == "GPI LIST" && (_starts_with(
        //                     _lower(v_gpi_no),
        //                     _lower(v_ndc_rec_ndc_gpi)
        //                   ) || v_ndc_rec_list_name == '-')) ||
        //                   (_string_upcase(v_rec_drug_type) == "NDC LIST"
        //                     &&
        //                     (_starts_with(
        //                       _lower(v_prod_id),
        //                       _lower(v_ndc_rec_ndc_gpi)
        //                     ) || v_ndc_rec_list_name == '-'))
        //                 ) 1
        //                 else
        //                   0

        //               val claim_flg =
        //                 if (
        //                   _datediff(
        //                     _now("yyyyMMdd"),
        //                     v_filled_dt
        //                   ) <= v_rec_lookback_days.toLong
        //                 ) 1
        //                 else 0
        //               val mbr_flg =
        //                 if (
        //                   _datediff(
        //                     v_mel_thrgh_dt,
        //                     _now("yyyyMMdd")
        //                   ) >= v_rec_mel_lookforward.toLong
        //                 ) 1
        //                 else 0

        //               c_valid_rec =
        //                 if (
        //                   v_rec_include_exclude == "I" && exact_inc_flg == 0 && exact_exc_flg == 0
        //                 ) {
        //                   if (drug_match == 0) 0
        //                   else if (drug_match == 1 && claim_flg == 1) 1
        //                   else 0
        //                 } else if (
        //                   v_rec_include_exclude == 'E' && exact_exc_flg == 0 && exact_inc_flg == 1
        //                 ) {
        //                   if (drug_match == 1 && claim_flg == 1) 0
        //                   else 1
        //                 } else if (
        //                   v_rec_include_exclude == 'E' && exact_exc_flg == 0
        //                 ) {
        //                   if (drug_match == 1 && claim_flg == 1) 0
        //                   else if (
        //                     drug_match == 1 && claim_flg == 0 && (rule_cnt != 0 && c_valid_rec == 1)
        //                   ) 1
        //                   else if (
        //                     drug_match == 0 && claim_flg == 1 && ((rule_cnt != 0 && c_valid_rec == 1) || rule_cnt == 0)
        //                   )
        //                     1
        //                   else 0
        //                 } else c_valid_rec;

        //               m_valid_rec =
        //                 if (
        //                   v_rec_include_exclude == 'I' && exact_inc_flg == 0 && exact_exc_flg == 0
        //                 ) {
        //                   if (drug_match == 0) 0
        //                   else if (drug_match == 1 && mbr_flg == 1) 1
        //                   else 0
        //                 } else if (
        //                   v_rec_include_exclude == 'E' && exact_exc_flg == 0 && exact_inc_flg == 1
        //                 ) {
        //                   if (drug_match == 1 && mbr_flg == 1) 0
        //                   else 1
        //                 } else if (
        //                   v_rec_include_exclude == 'E' && exact_exc_flg == 0
        //                 ) {
        //                   if (drug_match == 1 && mbr_flg == 1) 0
        //                   else if (
        //                     drug_match == 1 && mbr_flg == 0 && (rule_cnt != 0 && m_valid_rec == 1)
        //                   ) 1
        //                   else if (
        //                     drug_match == 0 && mbr_flg == 1 && ((rule_cnt != 0 && m_valid_rec == 1) || rule_cnt == 0)
        //                   ) 1
        //                   else 0
        //                 } else m_valid_rec;

        //               exact_inc_flg =
        //                 if (v_rec_include_exclude == 'I' && drug_match == 1) 1
        //                 else exact_inc_flg;
        //               exact_exc_flg =
        //                 if (v_rec_include_exclude == 'E' && drug_match == 1) 1
        //                 else exact_exc_flg;

        //               v_prior_auth_flg =
        //                 if (
        //                   (v_prior_auth_flg == 0 && _datediff(
        //                     v_mpa_thrgh_dt,
        //                     _now("yyyyMMdd")
        //                   ) == v_rec_pa_lookforward) || v_prior_auth_flg ==
        //                     1
        //                 ) 1
        //                 else 0;
        //               ndc_rule_cnt = ndc_rule_cnt + 1;
        //             }

        //           } else {
        //             val drug_match =
        //               if (
        //                 _isnull(v_rec_drug_type) || _is_blank(
        //                   v_rec_drug_type
        //                 ) || _first_defined(
        //                   v_rec_drug_type,
        //                   '-'
        //                 ) == '-' || (List("GPI LIST ", " NDC LIST")
        //                   .contains(_string_upcase(v_rec_drug_type))
        //                   && v_rec_drug_name_list == '-') || (_string_upcase(
        //                   v_rec_drug_type
        //                 ) == 'GPI && (_starts_with(
        //                   _lower(v_gpi_no),
        //                   _lower(v_rec_drug_name_list.toString)
        //                 ) || v_rec_drug_name_list == '-')) ||
        //                 (_string_upcase(v_rec_drug_type) == 'NDC
        //                   &&
        //                   (_starts_with(
        //                     _lower(v_prod_id),
        //                     _lower(v_rec_drug_name_list)
        //                   ) || v_rec_drug_name_list == '-'))
        //               ) 1
        //               else 0;

        //             val claim_flg =
        //               if (
        //                 _datediff(
        //                   _now("yyyyMMdd"),
        //                   v_filled_dt
        //                 ) <= v_rec_lookback_days.toLong
        //               ) 1
        //               else 0;
        //             val mbr_flg =
        //               if (
        //                 _datediff(
        //                   v_mel_thrgh_dt,
        //                   _now("yyyyMMdd")
        //                 ) >= v_rec_mel_lookforward.toLong
        //               ) 1
        //               else 0;

        //             c_valid_rec =
        //               if (
        //                 v_rec_include_exclude == 'I' && exact_inc_flg == 0 && exact_exc_flg == 0
        //               ) {
        //                 if (drug_match == 0) 0
        //                 else if (drug_match == 1 && claim_flg == 1) 1
        //                 else 0
        //               } else if (
        //                 v_rec_include_exclude == 'E' && exact_exc_flg == 0 && exact_inc_flg == 1
        //               ) {
        //                 if (drug_match == 1 && claim_flg == 1) 0
        //                 else 1
        //               } else if (
        //                 v_rec_include_exclude == 'E' && exact_exc_flg == 0
        //               ) {
        //                 if (drug_match == 1 && claim_flg == 1) 0
        //                 else if (
        //                   drug_match == 1 && claim_flg == 0 && (rule_cnt != 0 && c_valid_rec == 1)
        //                 ) 1
        //                 else if (
        //                   drug_match == 0 && claim_flg == 1 && ((rule_cnt != 0 && c_valid_rec == 1) || rule_cnt == 0)
        //                 ) 1
        //                 else 0
        //               } else c_valid_rec;

        //             m_valid_rec =
        //               if (
        //                 v_rec_include_exclude == 'I' && exact_inc_flg == 0 && exact_exc_flg == 0
        //               ) {
        //                 if (drug_match == 0) 0
        //                 else if (drug_match == 1 && mbr_flg == 1) 1
        //                 else 0
        //               } else if (
        //                 v_rec_include_exclude == 'E' && exact_exc_flg == 0 && exact_inc_flg == 1
        //               ) {
        //                 if (drug_match == 1 && mbr_flg == 1) 0
        //                 else 1
        //               } else if (
        //                 v_rec_include_exclude == 'E' && exact_exc_flg == 0
        //               ) {
        //                 if (drug_match == 1 && mbr_flg == 1) 0
        //                 else if (
        //                   drug_match == 1 && mbr_flg == 0 && (rule_cnt != 0 && m_valid_rec == 1)
        //                 ) 1
        //                 else if (
        //                   drug_match == 0 && mbr_flg == 1 && ((rule_cnt != 0 && m_valid_rec == 1) || rule_cnt == 0)
        //                 ) 1
        //                 else 0
        //               } else m_valid_rec;

        //             exact_inc_flg =
        //               if (v_rec_include_exclude == 'I' && drug_match == 1) 1
        //               else exact_inc_flg
        //             exact_exc_flg =
        //               if (v_rec_include_exclude == 'E' && drug_match == 1) 1
        //               else exact_exc_flg;
        //             v_prior_auth_flg =
        //               if (
        //                 (v_prior_auth_flg == 0 && _datediff(
        //                   v_mpa_thrgh_dt,
        //                   _now("yyyyMMdd")
        //                 ) == v_rec_pa_lookforward) || v_prior_auth_flg == 1
        //               ) 1
        //               else 0;
        //           }
        //           rule_cnt = rule_cnt + 1
        //         }
        //         c_valid_rec + "|" + m_valid_rec + "|" + v_prior_auth_flg
        //     },
        //     DoubleType
        //   )
        //     .withName("get_partial_drug_details_Udf")

        // spark.udf.register("sv_apply", sv_apply)
        // spark.udf.register("sv_create_collection", sv_create_collection)
        // spark.udf.register(
        //   "Get_full_day_of_week_from_number",
        //   Get_full_day_of_week_from_number
        // )
        // spark.udf.register("Get_Clean_Date_Local_Udf", Get_Clean_Date_Local_Udf)
        // spark.udf.register("Get_Ship_Date_Local_Udf", Get_Ship_Date_Local_Udf)
        // // spark.udf.register("Get_Holiday_Cnt_Values", Get_Holiday_Cnt_Values)
        // spark.udf.register("Weekend_Day_Cnt_Inner", Weekend_Day_Cnt_Inner)
        // spark.udf.register(
        //   "Get_V_Final_Days_To_Ship_Udf",
        //   Get_V_Final_Days_To_Ship_Udf
        // )
        // spark.udf.register("Get_V_Final_Tat_Hours_Udf", Get_V_Final_Tat_Hours_Udf)
        // spark.udf.register("Calculate_TAT_Hours_Udf", Calculate_TAT_Hours_Udf)
        // spark.udf.register("extract_mel_dates_Udf", extract_mel_dates_Udf)
        // spark.udf.register(
        //   "get_partial_drug_details_Udf",
        //   get_partial_drug_details_Udf
        // )

        var encrypt_key = "Secret not defined"
        var encrypt_iv = "Secret not defined"
        var decrypt_key = "Secret not defined"
        var decrypt_iv = "Secret not defined"

        import java.security.MessageDigest
        import java.util
        import javax.crypto.Cipher
        import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
        import org.apache.commons.codec.binary.Hex
        import org.apache.spark.sql.functions._

        try {

          // Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
          import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

          // loading db secrets
          encrypt_key = dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_EncKey
          )

          encrypt_iv = dbutils.secrets.get(
            scope = Config.encrypt_scope,
            key = Config.encrypt_InitVec
          )

          decrypt_key = dbutils.secrets.get(
            scope = Config.decrypt_scope,
            key = Config.decrypt_EncKey
          )

          decrypt_iv = dbutils.secrets.get(
            scope = Config.decrypt_scope,
            key = Config.decrypt_InitVec
          )
        } catch {
          case e: Exception => {
            println(
              "Please define databricks secrets for encrypt_key, decrypt_key, envrypt_iv and decrypt_iv on your cluster to use encrypt decrypt as udf"
            )
          }
        }

        def encrypt(key: String, ivString: String, plainValue: String): String = {
          if (plainValue != null) {
            val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
            cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key), getIVSpec(ivString))
            var encrypted_str =
              Hex.encodeHexString(cipher.doFinal(plainValue.getBytes("UTF-8")))
            encrypted_str = encrypted_str
            return encrypted_str
          } else {
            null
          }
        }

        def decrypt(
                     key: String,
                     ivString: String,
                     encryptedValue: String
                   ): String = {
          if (encryptedValue != null) {
            val cipher: Cipher = Cipher.getInstance("AES/OFB/PKCS5Padding")
            cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key), getIVSpec(ivString))
            new String(cipher.doFinal(Hex.decodeHex(encryptedValue.toCharArray())))
          } else {
            null
          }
        }

        def keyToSpec(key: String): SecretKeySpec = {
          var keyBytes: Array[Byte] = (key).getBytes("UTF-8")
          val sha: MessageDigest = MessageDigest.getInstance("MD5")
          keyBytes = sha.digest(keyBytes)
          keyBytes = util.Arrays.copyOf(keyBytes, 16)
          new SecretKeySpec(keyBytes, "AES")
        }

        def getIVSpec(IVString: String) = {
          new IvParameterSpec(IVString.getBytes())
        }

        val encryptUDF = udf(encrypt _)
        val decryptUDF = udf(decrypt _)
        spark.udf.register("aes_encrypt_udf", encryptUDF)
        spark.udf.register("aes_decrypt_udf", decryptUDF)

        println("Registered encrypt and decrypt UDFs in reforamt")

        var ff3_encrypt_key = "Secret not defined"
        var ff3_encrypt_tweak = "Secret not defined"

        try {

          // Encryption method for obfuscating PHI / PII fields defined in config encryptColumns
          import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

          // loading db secrets
          ff3_encrypt_key = dbutils.secrets.get(
            scope = Config.ff3_encrypt_scope,
            key = Config.ff3_encrypt_key
          )

          ff3_encrypt_tweak = dbutils.secrets.get(
            scope = Config.ff3_encrypt_scope,
            key = Config.ff3_encrypt_tweak
          )
        } catch {
          case e: Exception => {
            println(
              "Please define databricks secrets for ff3_encrypt_key and ff3_encrypt_tweak on your cluster to use ff3_encrypt_idwdata as udf"
            )
          }
        }

        def jsonStrToMap(jsonStr: String): Map[String, String] = {
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          parse(jsonStr).extract[Map[String, String]]
        }

        val col_values = jsonStrToMap(Config.reformat_rules.replace("\n", " "))

        var outputList = new ListBuffer[org.apache.spark.sql.Column]()

        col_values.foreach { case (key, value) =>
          outputList += expr(
            value
              .replace("SSSZ", "SSS")
              .replace(
                "STRING), 1, 20), 'T', -1), ' '), 'yyyy-MM-dd HH:mm:ss.'), ",
                "STRING), 1, 20), 'T', -1), ' '), 'yyyy-MM-dd HH:mm:ss'), "
              )
              .replace(
                "aes_encrypt_udf(",
                "aes_encrypt_udf('" + encrypt_key + "', '" + encrypt_iv + "', "
              )
              .replace(
                "aes_decrypt_udf(",
                "aes_decrypt_udf('" + decrypt_key + "', '" + decrypt_iv + "', "
              )
              .replace(
                "ff3_encrypt_idwdata(",
                "ff3_encrypt_idwdata('" + ff3_encrypt_key + "', '" + ff3_encrypt_tweak + "', "
              )
          ).as(key)
        }

        // val res = in0.persist(StorageLevel.DISK_ONLY)
        // res.count()

        in0.select(
          (outputList ++ List(
            col("file_name_timestamp"),
            col("source_file_base_path"),
            col("source_file_full_path"),
            col("dxf_src_sys_id")
          )): _*
        )
      } else {
        in0 // if no reformat rules are provided
      }
    out0
  }

}
