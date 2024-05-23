# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_moment_saver_bands";

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
# MAGIC SELECT index, Cashpot
# MAGIC     , count(distinct wallet_id) as Customer_Count
# MAGIC     , boost
# MAGIC     , count(distinct wallet_id) * boost as boost_cost
# MAGIC FROM (
# MAGIC select wallet_id, 
# MAGIC     case when curr_cash_bnk_pnts_qty < 10*100 then '<£10'
# MAGIC             when curr_cash_bnk_pnts_qty between 10*100 and 19.99*100 then '£10-£19.99'
# MAGIC             when curr_cash_bnk_pnts_qty between 20*100 and 24.99*100 then '£20-£24.99'
# MAGIC             when curr_cash_bnk_pnts_qty between 25*100 and 39.99*100 then '£25-£39.99'
# MAGIC             when curr_cash_bnk_pnts_qty between 40*100 and 49.99*100 then '£40-£49.99'
# MAGIC             when curr_cash_bnk_pnts_qty between 50*100 and 59.99*100 then '£50-£59.99'
# MAGIC             when curr_cash_bnk_pnts_qty between 60*100 and 69.99*100 then '£60-£69.99'
# MAGIC             when curr_cash_bnk_pnts_qty >= 70*100 then '>=£70'
# MAGIC             else 'Error'
# MAGIC             END AS Cashpot
# MAGIC     , curr_cash_bnk_pnts_qty
# MAGIC     , case when curr_cash_bnk_pnts_qty < 10*100 then '1'
# MAGIC             when curr_cash_bnk_pnts_qty between 10*100 and 19.99*100 then '2'
# MAGIC             when curr_cash_bnk_pnts_qty between 20*100 and 24.99*100 then '3'
# MAGIC             when curr_cash_bnk_pnts_qty between 25*100 and 39.99*100 then '4'
# MAGIC             when curr_cash_bnk_pnts_qty between 40*100 and 49.99*100 then '5'
# MAGIC             when curr_cash_bnk_pnts_qty between 50*100 and 59.99*100 then '6'
# MAGIC             when curr_cash_bnk_pnts_qty between 60*100 and 69.99*100 then '7'
# MAGIC             when curr_cash_bnk_pnts_qty >= 70*100 then '8'
# MAGIC             else 'Error'
# MAGIC             END AS index
# MAGIC     , case when curr_cash_bnk_pnts_qty < 10*100 then 0
# MAGIC             when curr_cash_bnk_pnts_qty between 10*100 and 19.99*100 then 1
# MAGIC             when curr_cash_bnk_pnts_qty between 20*100 and 24.99*100 then 2
# MAGIC             when curr_cash_bnk_pnts_qty between 25*100 and 39.99*100 then 3 --need to check these
# MAGIC             when curr_cash_bnk_pnts_qty between 40*100 and 49.99*100 then 5
# MAGIC             when curr_cash_bnk_pnts_qty between 50*100 and 59.99*100 then 5
# MAGIC             when curr_cash_bnk_pnts_qty between 60*100 and 69.99*100 then 5
# MAGIC             when curr_cash_bnk_pnts_qty >= 70*100 then 5
# MAGIC             else 0
# MAGIC             END AS boost
# MAGIC from ${widget.core_catalog}.gb_mb_dl_tables.cashpot_bal
# MAGIC where scheme_id = 531227 and curr_cash_bnk_pnts_qty!= 0)
# MAGIC GROUP BY Cashpot, index, boost
# MAGIC order by index asc;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
