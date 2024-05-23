# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create - vw_wallet_pos_txns

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_wallet_pos_txns";

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

# MAGIC %md
# MAGIC ###Create view query
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC   SELECT
# MAGIC   *
# MAGIC From
# MAGIC   (
# MAGIC     select
# MAGIC       wallet_id,
# MAGIC       store_nbr,
# MAGIC       event_ts,CASE
# MAGIC         WHEN CAST(EVENT_TS AS DATE) BETWEEN DATE_ADD(CURRENT_DATE, -7)
# MAGIC         AND CURRENT_DATE THEN wallet_id
# MAGIC         ELSE 0
# MAGIC       END AS CURRENT_7_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN CAST(EVENT_TS AS DATE) BETWEEN DATE_ADD(CURRENT_DATE, -8)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -2) THEN wallet_id
# MAGIC         ELSE 0
# MAGIC       END AS YESTERDAY_7_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN CAST(EVENT_TS AS DATE) BETWEEN DATE_ADD(CURRENT_DATE, -14)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -8) THEN wallet_id
# MAGIC         ELSE 0
# MAGIC       END AS ROLLING_7_DAY_ACTIVE_FLAG
# MAGIC     from
# MAGIC     ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     where
# MAGIC       1 = 1
# MAGIC       and cast(event_ts as date) < CURRENT_DATE
# MAGIC       and cast(event_ts as date) >= cast('2021-09-05' as date)
# MAGIC   ) --remove test stores that have alphabetic characters
# MAGIC where
# MAGIC   try_cast(store_nbr as int) IS NOT NULL;
# MAGIC
# MAGIC
# MAGIC --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
