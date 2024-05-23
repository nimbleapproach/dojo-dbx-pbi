# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_rns_exc_expr_timeseries";
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
# MAGIC WITH raw_dates AS (
# MAGIC
# MAGIC     SELECT cd.day_date as full_date
# MAGIC     FROM ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar cd
# MAGIC     WHERE cd.day_date <= current_date() 
# MAGIC           and cd.day_date >= cast('2022-08-01' as date) 
# MAGIC           --and cd.day_date >= cast('2023-02-16' as date)
# MAGIC )  
# MAGIC
# MAGIC , wallet_pos_first as (
# MAGIC SELECT wallet_id
# MAGIC     , min(event_ts) as first_scan
# MAGIC FROM ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns
# MAGIC GROUP BY wallet_id
# MAGIC )
# MAGIC
# MAGIC ,loyalty_first_scans as (
# MAGIC SELECT la.wallet_id, first_scan, acct_del_ind_1, regtn_ts
# MAGIC FROM ${widget.catalog}.${widget.schema}.vw_loyalty_acct la
# MAGIC LEFT OUTER JOIN wallet_pos_first wpt
# MAGIC ON la.wallet_id = wpt.wallet_id)
# MAGIC
# MAGIC
# MAGIC SELECT full_date, count(distinct wallet_id) as wallet_id
# MAGIC FROM raw_dates rd
# MAGIC LEFT JOIN loyalty_first_scans
# MAGIC ON (first_scan >= rd.full_date or first_scan IS NULL) and regtn_ts < full_date and  (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(full_date, -37) )) --this excludes expired guests
# MAGIC GROUP BY full_date;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
