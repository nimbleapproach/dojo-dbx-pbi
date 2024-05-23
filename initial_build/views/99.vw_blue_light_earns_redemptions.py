# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_blue_light_earns_redemptions";

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
# MAGIC select cmpgn_cmplt_dt as cmpgn_cmplt_dt
# MAGIC     , store_nbr as store_nbr 
# MAGIC     , redemptions as redemptions
# MAGIC     , cast(earned as float)/100 as earned
# MAGIC     , 0 as cumu_redemptions -- kept in to stop power BI freakout
# MAGIC     , 0 as cumu_earned
# MAGIC from(
# MAGIC     select cast(cmpgn_cmplt_ts as date) as cmpgn_cmplt_dt
# MAGIC         , store_nbr
# MAGIC         , count(*) as redemptions
# MAGIC         , sum(rdmpt_val) as earned
# MAGIC     from ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets mw
# MAGIC     left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt -- to get store number
# MAGIC             on mw.wallet_id = wpt.wallet_id
# MAGIC             and cast(mw.cmpgn_cmplt_ts as timestamp) = cast(wpt.event_ts as timestamp)
# MAGIC     WHERE mw.cmpgn_id = 508017 and cast(cmpgn_cmplt_ts as date) < current_date() and mssn_status_desc NOT LIKE 'CANCELLED'
# MAGIC     GROUP BY cmpgn_cmplt_dt, store_nbr)
# MAGIC ORDER BY store_nbr, cmpgn_cmplt_dt;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
