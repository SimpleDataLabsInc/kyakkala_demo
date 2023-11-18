
val df_spark_configs = spark_configs(context)
val df_generate_lookups =
  generate_lookups(context, df_spark_configs)
val df_read_source = read_source(context, df_generate_lookups)
val df_cleanse_hex = cleanse_hex(context, df_read_source).cache()
val df_optional_filter_and_audit_fields =
  optional_filter_and_audit_fields(context, df_cleanse_hex)
val df_optional_repartition = optional_repartition(
  context,
  df_optional_filter_and_audit_fields
).cache()
val df_put_src_env_sk =
  put_src_env_sk(context, df_optional_repartition)
val df_apply_source_default =
  apply_source_default(context, df_put_src_env_sk)
val df_optional_joins =
  optional_joins(context, df_apply_source_default)
val df_optional_filter_rules =
  optional_filter_rules(context, df_optional_joins)
val df_optional_normalize =
  optional_normalize(context, df_optional_filter_rules)
val df_optional_order_by =
  optional_order_by(context, df_optional_normalize)
val df_optional_default_rules =
  optional_default_rules(context, df_optional_order_by)
val df_apply_reformat_rules =
  apply_reformat_rules(context, df_optional_default_rules).cache()
val df_self_join_sk =
  self_join_sk(context, df_apply_reformat_rules)
val df_survivorship_rule =
  survivorship_rule(context, df_self_join_sk)
val (df_apply_len_rules_out0, df_apply_len_rules_out1) = {
  val (df_apply_len_rules_out0_temp, df_apply_len_rules_out1_temp) =
    apply_len_rules(context, df_survivorship_rule)
  (df_apply_len_rules_out0_temp
    .cache(),
    df_apply_len_rules_out1_temp
      .cache()
  )
}
reject_records(context, df_apply_len_rules_out1)
val df_dedup_filter =
  dedup_filter(context, df_apply_len_rules_out0)
val df_apply_dedup_rules =
  apply_dedup_rules(context, df_dedup_filter)
val df_optional_rollup =
  optional_rollup(context, df_apply_dedup_rules)
val df_apply_default_rules =
  apply_default_rules(context, df_optional_rollup).cache()
val df_get_sk = get_sk(context, df_apply_default_rules).cache()
val df_generate_sk_service_col =
  generate_sk_service_col(context, df_get_sk).cache()
val df_add_audit_cols =
  add_audit_cols(context, df_generate_sk_service_col)
val df_optional_post_filter =
  optional_post_filter(context, df_add_audit_cols)
val df_select_final_cols =
  select_final_cols(context, df_optional_post_filter)
val df_encryption = encryption(context, df_select_final_cols).cache()