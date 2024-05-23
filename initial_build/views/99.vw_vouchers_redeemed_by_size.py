# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_vouchers_redeemed_by_size";
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

# COMMAND -----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC select case when voucher_value_pounds <= 15 then voucher_value_pounds
# MAGIC         when voucher_value_pounds > 15 then '15+'
# MAGIC         end as voucher_value_pounds
# MAGIC             , sum(num_ppl) as num_ppl
# MAGIC     from(
# MAGIC         select voucher_value_pounds 
# MAGIC             , count(wallet_id) as num_ppl
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_reward_wallets
# MAGIC         where reward_redm_ts is not null
# MAGIC         group by voucher_value_pounds
# MAGIC         order by voucher_value_pounds
# MAGIC             )
# MAGIC
# MAGIC     group by voucher_value_pounds
# MAGIC     order by voucher_value_pounds;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
