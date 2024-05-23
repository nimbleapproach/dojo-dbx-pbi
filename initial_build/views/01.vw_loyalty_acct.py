# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_loyalty_acct";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
spark.conf.set ('widget.view_name', dbutils.widgets.get("view_name"))


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.text('view_name', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.core_catalog}' AS core_catalog,
# MAGIC        '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS schema,
# MAGIC        '${widget.view_name}' AS view_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC select *,
# MAGIC case when cashbank_categorised = '£0' then 1
# MAGIC     when cashbank_categorised = '£0-£0.99' then 2
# MAGIC     when cashbank_categorised = '£1-£1.99' then 3
# MAGIC     when cashbank_categorised = '£2-£2.99' then 4
# MAGIC     when cashbank_categorised = '£3-£3.99' then 5
# MAGIC     when cashbank_categorised = '£4-£4.99' then 6
# MAGIC     when cashbank_categorised = '£5-£9.99' then 7
# MAGIC     when cashbank_categorised = '£10-£19.99' then 8
# MAGIC     when cashbank_categorised = '£20+' then 9
# MAGIC     else null
# MAGIC     end as cashbank_categorised_order 
# MAGIC  from(
# MAGIC select wallet_id
# MAGIC     ,singl_profl_id
# MAGIC     ,reg_dt
# MAGIC     ,curr_cash_bnk_pnts_qty
# MAGIC     ,lifetime_pnts_qty
# MAGIC     ,last_shopped_store_id
# MAGIC     ,acct_status_id
# MAGIC     ,dt_cnvrt_to_full_acct_ts
# MAGIC     ,regtn_ts
# MAGIC     ,case when wallet_id_wpt is null then 0 else 1
# MAGIC     end as active_ind
# MAGIC     , case when regtn_ts <> dt_cnvrt_to_full_acct_ts then 1
# MAGIC         when dt_cnvrt_to_full_acct_ts is null  then 1 else 0 
# MAGIC     end as guest_ind -- this field flags any account that has ever been a guest- used when calculating conversion rate from guest to perm
# MAGIC     ,case when curr_cash_bnk_pnts_qty = 0 then '£0'
# MAGIC     when (curr_cash_bnk_pnts_qty > 0 and curr_cash_bnk_pnts_qty < 100) then '£0-£0.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 100 and curr_cash_bnk_pnts_qty < 200) then '£1-£1.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 200 and curr_cash_bnk_pnts_qty < 300) then '£2-£2.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 300 and curr_cash_bnk_pnts_qty < 400) then '£3-£3.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 400 and curr_cash_bnk_pnts_qty < 500) then '£4-£4.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 500 and curr_cash_bnk_pnts_qty < 1000) then '£5-£9.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 1000 and curr_cash_bnk_pnts_qty < 2000) then '£10-£19.99'
# MAGIC     when (curr_cash_bnk_pnts_qty >= 2000) then '£20+'
# MAGIC     else null
# MAGIC     end as cashbank_categorised
# MAGIC     ,acct_del_ind_1
# MAGIC     , src_modfd_ts -- added by James C for use in incremental refresh
# MAGIC from( 
# MAGIC     select distinct la.wallet_id
# MAGIC         ,case when singl_profl_id is not null then singl_profl_id
# MAGIC         when singl_profl_id is null then concat('no_assgnd_spid_', cast   (la.wallet_id as varchar(50)))
# MAGIC         end as singl_profl_id
# MAGIC         ,cast(regtn_ts as date) as reg_dt
# MAGIC         ,curr_cash_bnk_pnts_qty
# MAGIC         ,lifetime_pnts_qty
# MAGIC         ,last_shopped_store_id
# MAGIC         ,acct_status_id
# MAGIC         ,dt_cnvrt_to_full_acct_ts
# MAGIC         ,regtn_ts
# MAGIC         ,wpt.wallet_id as wallet_id_wpt
# MAGIC         ,case when ((regtn_ts< cast(current_timestamp as timestamp)- INTERVAL 888 HOURS) and acct_status_id='TEMP') then 1 else 0
# MAGIC         end as acct_del_ind_1
# MAGIC         , la.src_modfd_ts -- added by James C for use in incremental refresh
# MAGIC     from ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC     left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     on la.wallet_id=wpt.wallet_id
# MAGIC     and cast(la.regtn_ts as date)=cast(wpt.event_ts as date)
# MAGIC     where 1=1
# MAGIC     and cast(regtn_ts as date) < CURRENT_DATE and cast(regtn_ts as date) >= cast('2021-09-05' as date)
# MAGIC ));
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
