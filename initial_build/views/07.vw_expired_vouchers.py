# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_expired_vouchers";

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
# MAGIC         cmpgn_id
# MAGIC         ,cast(reward_end_ts as date) reward_end_dt
# MAGIC         ,null as store_nbr
# MAGIC         ,sum(case when (reward_redm_ts is null and reward_end_ts< current_timestamp()) then 1 else 0 end) vouchers_expired
# MAGIC         ,sum(case when (reward_redm_ts is null and reward_end_ts< current_timestamp()) then voucher_value_pounds else 0 end) voucher_value_pounds_expired
# MAGIC         ,chnl_nm
# MAGIC         ,acct_status_id
# MAGIC from ${widget.catalog}.${widget.schema}.vw_reward_wallets a
# MAGIC where reward_redm_ts is null
# MAGIC     and reward_end_ts< current_timestamp()
# MAGIC group by
# MAGIC         cmpgn_id
# MAGIC         ,cast(reward_end_ts as date)
# MAGIC         ,store_nbr
# MAGIC         ,chnl_nm
# MAGIC         ,acct_status_id
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
