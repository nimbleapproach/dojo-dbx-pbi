# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_sign_ups_aggregated";
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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC select marvel_signup_dt
# MAGIC     , count(singl_profl_id) as num_sign_ups
# MAGIC     , sum(new_ind) as num_new_ind
# MAGIC     , sum(opt_in_flag) as num_opt_in_flag
# MAGIC     , sum(acct_del_ind_1) as num_acct_del
# MAGIC     , acct_status_id
# MAGIC     , sum(opt_in_flag_2) as opt_in_perm_only
# MAGIC     , sum(new_ind_2) as new_ind_perm_only
# MAGIC
# MAGIC from (select *
# MAGIC             -- both opt_in and new_ind fields are filtered for permanent accounts only
# MAGIC             , case when (acct_status_id = 'DEFAULT' and opt_in_flag=1) then 1 else 0 end as opt_in_flag_2
# MAGIC             , case when (acct_status_id = 'DEFAULT' and new_ind=1) then 1 else 0 end as new_ind_2
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_sign_ups)
# MAGIC
# MAGIC group by marvel_signup_dt
# MAGIC         , acct_status_id;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
