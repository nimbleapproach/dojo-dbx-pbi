# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_one_off_campaigns";

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
# MAGIC (SELECT 'Star Product' as promotion_type, '10% on all George Clothing (ex George.com)' as campaign_name,  452114 as campaign_id,  cast('2022-08-31' as date) as campaign_start_date,  cast('2022-09-14' as date) as campaign_end_date,  0.1754 as rewards_participation,  0.0193 as epos_sales_participation,  0.1022 as giveaway_perc,  1056512 as rewards_redemption_volume,  996783.51 as rewards_cash_pot,  0.2437 as rewards_customer_penetration,  6024568 as total_epos_volume
# MAGIC UNION 
# MAGIC SELECT'Star Product',    '10% on all George Clothing (ex George.com)',  483567,  CAST('2022-10-23' as date), CAST('2022-11-6' as DATE),  0.2184,  0.0226,  0.1009,  1223144,  1142141.29, 0.2240,  5599237
# MAGIC UNION
# MAGIC SELECT 'Star Product',    '10% Baby Event',                              492819,  CAST('2023-1-4' as DATE), CAST('2023-1-29' AS DATE),  0.3701,  0.0327,  0.0964,  3663624,  912340.19,  0.2476,  9899138)
# MAGIC ;
