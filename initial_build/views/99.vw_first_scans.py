# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_first_scans";

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
# MAGIC with first_scans as (
# MAGIC     SELECT wpt.wallet_id, cast(min(wpt.event_ts) as date) as first_scan_dt
# MAGIC     FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     JOIN ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC     on wpt.wallet_id = la.wallet_id
# MAGIC     GROUP BY wpt.wallet_id)
# MAGIC
# MAGIC SELECT first_scan_dt
# MAGIC     , num_people
# MAGIC     , SUM(num_people) OVER (ORDER BY first_scan_dt asc) as cumu_people
# MAGIC
# MAGIC FROM(
# MAGIC SELECT first_scan_dt, count(wallet_id) as num_people
# MAGIC FROM first_scans
# MAGIC WHERE first_scan_dt < current_date()
# MAGIC GROUP BY first_scan_dt
# MAGIC
# MAGIC ORDER BY first_scan_dt);
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
