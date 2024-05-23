# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_blue_light_activity_funnel";

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
# MAGIC WITH blue_lyl_acct AS (
# MAGIC     SELECT la.wallet_id
# MAGIC         , la.singl_profl_id
# MAGIC         , la.bluelight_id
# MAGIC         , coalesce(cast(laa.bluelight_id_last_upd_ts as DATE), cast(la.bluelight_id_last_upd_ts as DATE)) as blue_light_upd_ts
# MAGIC         , last_scan
# MAGIC     FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC     LEFT OUTER JOIN(
# MAGIC         SELECT wallet_id
# MAGIC             , min(bluelight_id_last_upd_ts) as bluelight_id_last_upd_ts
# MAGIC         FROM  ${widget.catalog}.${widget.schema}.loyalty_acct_archive
# MAGIC         WHERE bluelight_id_last_upd_ts IS NOT NULL
# MAGIC         GROUP BY wallet_id
# MAGIC         )laa
# MAGIC     ON la.wallet_id = laa.wallet_id 
# MAGIC
# MAGIC
# MAGIC     JOIN(
# MAGIC         SELECT wallet_id
# MAGIC             , max(event_ts) as last_scan
# MAGIC         FROM coreprod.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC         WHERE event_ts < current_date() and event_ts >= cast('2023-05-03' as date) and chnl_nm LIKE 'store'
# MAGIC         GROUP BY wallet_id
# MAGIC     ) wpts
# MAGIC     ON la.wallet_id = wpts.wallet_id
# MAGIC         AND coalesce(cast(laa.bluelight_id_last_upd_ts as DATE), cast(la.bluelight_id_last_upd_ts as DATE)) <= wpts.last_scan --abit unsure on how this logic will hold up after they might have expired, i.e. miost recent scan might be after it expired so it won't count them anymore. Have to see?
# MAGIC     WHERE la.bluelight_id_last_upd_ts IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT sum(scanned_total) as scanned_total
# MAGIC     , sum(scanned_30day) as scanned_30day
# MAGIC     , sum(scanned_7day) as scanned_7day
# MAGIC     , sum(scanned_1day) as scanned_1day
# MAGIC FROM(
# MAGIC     SELECT wallet_id, 
# MAGIC         CASE WHEN last_scan >= cast('2023-05-03' as date) THEN 1 else 0 end as scanned_total
# MAGIC         , CASE WHEN last_scan >= date_add(current_date(), -30) THEN 1 else 0 end as scanned_30day
# MAGIC         , CASE WHEN last_scan >= date_add(current_date(), -7) THEN 1 else 0 end as scanned_7day
# MAGIC         , CASE WHEN last_scan >= date_add(current_date(), -1) THEN 1 else 0 end as scanned_1day
# MAGIC     FROM blue_lyl_acct
# MAGIC )
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
# MAGIC
