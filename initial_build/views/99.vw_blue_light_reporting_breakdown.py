# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_blue_light_reporting_breakdown";

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
# MAGIC with blue_light_users as (
# MAGIC     SELECT la.wallet_id
# MAGIC     , la.singl_profl_id
# MAGIC     , la.bluelight_id
# MAGIC     , cast(la.regtn_ts as DATE) as regtn_ts
# MAGIC     , cast(la.bluelight_id_last_upd_ts as DATE) as blue_light_upd_ts
# MAGIC     , scan
# MAGIC FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC LEFT JOIN  
# MAGIC     (select wallet_id
# MAGIC         , cast(max(event_ts) as date) as scan 
# MAGIC     FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC     --where chnl_nm LIKE 'store'
# MAGIC     GROUP BY wallet_id
# MAGIC         ) wpt -- to get store number
# MAGIC
# MAGIC ON la.wallet_id = wpt.wallet_id
# MAGIC WHERE bluelight_id_last_upd_ts IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT  b.*, c.New 
# MAGIC FROM 
# MAGIC (
# MAGIC
# MAGIC SELECT 'Index' as Group, 2 as Existing
# MAGIC UNION
# MAGIC -- bottom left
# MAGIC SELECT 'Grand Total' as Group, count(wallet_id) as Existing FROM blue_light_users WHERE regtn_ts <= cast('2023-05-02' as date)
# MAGIC UNION
# MAGIC SELECT 'Users Scanned Post inc. 3rd' as Group, count(wallet_id) as Existing FROM blue_light_users WHERE scan >= cast('2023-05-03' as date) and regtn_ts <= cast('2023-05-02' as date)
# MAGIC UNION
# MAGIC SELECT 'Users Scanned Pre 3rd' as Group, count(wallet_id) as Existing FROM blue_light_users WHERE scan < cast('2023-05-03' as date) and regtn_ts <= cast('2023-05-02' as date)
# MAGIC UNION
# MAGIC SELECT 'Users Not Scanned' as Group, count(wallet_id) as Existing FROM blue_light_users WHERE scan is NULL and regtn_ts <= cast('2023-05-02' as date)) b
# MAGIC --ON a.Group = b.Group
# MAGIC JOIN
# MAGIC --bottom right
# MAGIC (
# MAGIC
# MAGIC SELECT 'Index' as Group, 1 as New
# MAGIC UNION
# MAGIC SELECT 'Grand Total' as Group, count(wallet_id) as New FROM blue_light_users WHERE regtn_ts > cast('2023-05-02' as date)
# MAGIC UNION
# MAGIC SELECT 'Users Scanned Post inc. 3rd' as Group, count(wallet_id) as New FROM blue_light_users WHERE scan IS NOT NULL and regtn_ts > cast('2023-05-02' as date)
# MAGIC UNION
# MAGIC SELECT 'Users Scanned Pre 3rd' as Group, null as New FROM blue_light_users 
# MAGIC UNION
# MAGIC SELECT 'Users Not Scanned' as Group, count(wallet_id) as New FROM blue_light_users WHERE scan is NULL and regtn_ts > cast('2023-05-02' as date)
# MAGIC ) c
# MAGIC ON b.Group = c.Group;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
