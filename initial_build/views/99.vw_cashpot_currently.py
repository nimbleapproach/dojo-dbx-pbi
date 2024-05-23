# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_cashpot_currently";

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
# MAGIC with summary_tab as (
# MAGIC     SELECT wallet_id
# MAGIC         , 1 as total
# MAGIC         , case when curr_cash_bnk_pnts_qty >= 10*100 then 1 else 0 end as ten_count
# MAGIC         , case when curr_cash_bnk_pnts_qty >= 20*100 then 1 else 0 end as twenty_count
# MAGIC         , case when curr_cash_bnk_pnts_qty >= 10*100 then curr_cash_bnk_pnts_qty else 0 end as ten_cash
# MAGIC         , case when curr_cash_bnk_pnts_qty >= 20*100 then curr_cash_bnk_pnts_qty else 0 end as twenty_cash
# MAGIC     FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct
# MAGIC     WHERE (acct_status_id LIKE 'DEFAULT' 
# MAGIC                 OR (acct_status_id LIKE 'TEMP' AND cast(regtn_ts as date) >= date_add(current_date(), -37)))
# MAGIC )
# MAGIC , summary_agg as (
# MAGIC     select sum(total) as total
# MAGIC         , sum(ten_count) as ten_count
# MAGIC         , sum(twenty_count) as twenty_count
# MAGIC         , sum(ten_cash) as ten_cash
# MAGIC         , sum(twenty_cash) as twenty_cash
# MAGIC     from summary_tab
# MAGIC )
# MAGIC
# MAGIC select 'Current cashpot greater than (or equal to) £10' as Desc
# MAGIC     , ten_count as num_wallets
# MAGIC     , (cast(ten_count as float)/cast(total as float))  as percentage_of_total
# MAGIC     , cast(ten_cash as float)/100 as curr_cashpot
# MAGIC from summary_agg
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC select 'Current cashpot greater than (or equal to) £20' as Desc
# MAGIC     , twenty_count as num_wallets
# MAGIC     , (cast(twenty_count as float)/cast(total as float))  as percentage_of_total
# MAGIC     , cast(twenty_cash as float)/100 as curr_cashpot
# MAGIC from summary_agg
# MAGIC
# MAGIC order by Desc
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
# MAGIC
