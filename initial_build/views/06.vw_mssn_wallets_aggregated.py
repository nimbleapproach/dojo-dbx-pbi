# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_mssn_wallets_aggregated";
# MAGIC

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

# COMMAND -------------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC select cmpgn_id
# MAGIC     , cast(cmpgn_cmplt_ts as date) cmpgn_cmplt_dt
# MAGIC     , store_nbr
# MAGIC     , chnl_nm
# MAGIC     , activity_type
# MAGIC     , acct_status_id
# MAGIC     , sum(rdmpt_cnt) as rdmpt_cnt
# MAGIC     , sum(mission_value_pounds_new) as earn_value_gbp
# MAGIC     , null as expr --required to stop Power BI freaking out over missing fields - previous versions had an extra field here. 
# MAGIC from ${widget.catalog}.${widget.schema}.vw_mssn_wallets
# MAGIC group by 
# MAGIC     cmpgn_id
# MAGIC     , cast(cmpgn_cmplt_ts as date)
# MAGIC     , store_nbr
# MAGIC     , chnl_nm
# MAGIC     , activity_type
# MAGIC     , acct_status_id;
# MAGIC
# MAGIC

