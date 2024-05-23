# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_blue_light_users_timeseries";

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
# MAGIC WITH blue_lyl_acct AS
# MAGIC (SELECT la.wallet_id
# MAGIC     , la.singl_profl_id
# MAGIC     , la.bluelight_id
# MAGIC     , cast(la.regtn_ts as DATE) as regtn_ts
# MAGIC     ,  coalesce(cast(laa.bluelight_id_last_upd_ts as DATE), cast(la.bluelight_id_last_upd_ts as DATE)) as blue_light_upd_ts
# MAGIC     , CASE WHEN cast(la.regtn_ts AS DATE) > cast('2023-05-02' as date) THEN 'New to Rewards' ELSE 'Existed in Rewards' END AS new_rewards
# MAGIC FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC LEFT OUTER JOIN(
# MAGIC     select wallet_id
# MAGIC     , min(bluelight_id_last_upd_ts) as bluelight_id_last_upd_ts
# MAGIC     from ${widget.catalog}.${widget.schema}.loyalty_acct_archive laa
# MAGIC     WHERE bluelight_id_last_upd_ts is not null
# MAGIC     GROUP BY wallet_id
# MAGIC)    laa
# MAGIC       ON la.wallet_id = laa.wallet_id


# MAGIC WHERE la.bluelight_id_last_upd_ts IS NOT NULL)
# MAGIC
# MAGIC SELECT * 
# MAGIC     , SUM(cumu_users) OVER (PARTITION BY blue_light_upd_ts) as cumu__total_users
# MAGIC FROM
# MAGIC (
# MAGIC SELECT * 
# MAGIC     , SUM(num_users) OVER (PARTITION BY blue_light_upd_ts) as total_users
# MAGIC     , SUM(num_users) OVER (PARTITION BY new_rewards ORDER BY blue_light_upd_ts) as cumu_users
# MAGIC FROM
# MAGIC     (SELECT blue_light_upd_ts, new_rewards, count(wallet_id) as num_users
# MAGIC     FROM blue_lyl_acct
# MAGIC     WHERE blue_light_upd_ts < current_date()
# MAGIC     GROUP BY blue_light_upd_ts, new_rewards
# MAGIC     ORDER BY  blue_light_upd_ts, new_rewards))
# MAGIC
# MAGIC ORDER BY blue_light_upd_ts, new_rewards
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
# MAGIC
