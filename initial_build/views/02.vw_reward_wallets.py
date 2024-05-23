# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_reward_wallets";
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
# MAGIC SELECT 
# MAGIC     rwd.*
# MAGIC     ,ly.acct_status_id
# MAGIC FROM
# MAGIC (
# MAGIC     select rw.wallet_id
# MAGIC         ,rw.cmpgn_id
# MAGIC         ,reward_gained_ts
# MAGIC         ,reward_end_ts
# MAGIC         ,reward_redm_ts
# MAGIC         ,cs.pound_value as voucher_value_pounds
# MAGIC         ,store_nbr
# MAGIC         ,cmpgn_type_desc
# MAGIC         ,wpt.chnl_nm
# MAGIC     from ${widget.core_catalog}.gb_mb_dl_tables.reward_wallets rw
# MAGIC     left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     on rw.wallet_id = wpt.wallet_id
# MAGIC         and rw.reward_redm_ts = wpt.event_ts
# MAGIC     left join ${widget.catalog}.${widget.schema}.vw_cmpgn_setup cs
# MAGIC     on rw.cmpgn_id = cs.cmpgn_id
# MAGIC     where 1=1  
# MAGIC     and rec_status_ind = 'CURRENT'
# MAGIC     and cast(reward_gained_ts as date) < CURRENT_DATE
# MAGIC     and cast(reward_gained_ts as date) >= cast('2021-09-05' as date)
# MAGIC     and (cast(reward_redm_ts as date) < CURRENT_DATE or reward_redm_ts is NULL)
# MAGIC     and (cast(reward_redm_ts as date) >= cast('2021-09-05' as date) or reward_redm_ts is NULL)
# MAGIC     and not (rw.wallet_id = 66819689)
# MAGIC ) rwd
# MAGIC
# MAGIC LEFT OUTER JOIN ${widget.catalog}.${widget.schema}.vw_loyalty_acct ly
# MAGIC     ON rwd.wallet_id = ly.wallet_id ;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
