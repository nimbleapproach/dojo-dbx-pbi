# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_daily_rewards_scanners";

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
# MAGIC         /*
# MAGIC         -- Asda Rewards Trading Dashboard - Daily Rewards Scanners
# MAGIC         -- split by Total/Store/GHS - WS 2023-08-10
# MAGIC         */
# MAGIC
# MAGIC         select to_date(event_ts) as Scan_Date, case when upper(chnl_nm) = 'ECOM' then 'GHS Only' when upper(chnl_nm) = 'STORE' then 'In-store Only' else '???' end as chnl_nm, count(distinct wallet_id) as Scanners
# MAGIC         from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC         where to_date(event_ts) between '2022-06-01' and date_add(current_date(),-1)
# MAGIC         group by 1,2
# MAGIC         union
# MAGIC         select to_date(event_ts) as Scan_Date, 'All' as chnl_nm, count(distinct wallet_id) as Scanners
# MAGIC         from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC         where to_date(event_ts) between '2022-06-01' and date_add(current_date(),-1)
# MAGIC         group by 1,2
# MAGIC         order by 1,2
# MAGIC

# COMMAND ----------


