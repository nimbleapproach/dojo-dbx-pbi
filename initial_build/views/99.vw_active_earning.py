# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_active_earning";

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
# MAGIC select
# MAGIC   active_earning_ytd,
# MAGIC   active_earning_7days
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       cast(wallets_earning as float) / cast(wallets_active as float) as active_earning_ytd,
# MAGIC       join1
# MAGIC     from
# MAGIC       -- Count number of distinct wallets that have completed a cmpgn in past 365 days
# MAGIC       (
# MAGIC         select
# MAGIC           count(distinct wallet_id) as wallets_earning,
# MAGIC           1 as join1
# MAGIC         from
# MAGIC           ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets
# MAGIC         where
# MAGIC           cast(cmpgn_cmplt_ts as date) >= date_sub(current_date, 365)
# MAGIC       ) a
# MAGIC       join -- Count number of distinct wallets which have scanned in the past 365 days
# MAGIC       (
# MAGIC         select
# MAGIC           count(distinct wallet_id) as wallets_active,
# MAGIC           1 as join2
# MAGIC         from
# MAGIC           ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC         where
# MAGIC           cast(event_ts as date) >= date_sub(current_date, 365)
# MAGIC       ) b on a.join1 = b.join2
# MAGIC   ) i
# MAGIC   join -- As above, but only looking at wallets which have earned / been scanned in the past 7 days (above is the past 365 days)
# MAGIC   (
# MAGIC     select
# MAGIC       cast(wallets_earning as float) / cast(wallets_active as float) as active_earning_7days,
# MAGIC       join1
# MAGIC     from(
# MAGIC         select
# MAGIC           count(distinct wallet_id) as wallets_earning,
# MAGIC           1 as join1
# MAGIC         from
# MAGIC           ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets
# MAGIC         where
# MAGIC           cast(cmpgn_cmplt_ts as date) >= date_sub(current_date, 7)
# MAGIC       ) a
# MAGIC       join (
# MAGIC         select
# MAGIC           count(distinct wallet_id) as wallets_active,
# MAGIC           1 as join2
# MAGIC         from
# MAGIC           ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC         where
# MAGIC           cast(event_ts as date) >= date_sub(current_date, 7)
# MAGIC       ) b on a.join1 = b.join2
# MAGIC   ) j on i.join1 = j.join1;
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
