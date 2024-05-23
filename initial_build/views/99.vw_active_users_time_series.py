# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_active_users_time_series";

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
# MAGIC with sign_ups_cumulative_CTE as(
# MAGIC     select 
# MAGIC         marvel_signup_dt
# MAGIC         , sum(num_sign_ups) over (order by marvel_signup_dt) sign_ups_cum 
# MAGIC     from ${widget.catalog}.${widget.schema}.vw_sign_ups_aggregated
# MAGIC )
# MAGIC
# MAGIC ,raw_dates AS (
# MAGIC   
# MAGIC       SELECT cd.day_date as full_date
# MAGIC       FROM ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar cd
# MAGIC       WHERE cd.day_date < current_date() 
# MAGIC             and cd.day_date >= cast('2022-08-01' as date)
# MAGIC   )  
# MAGIC
# MAGIC ,active_cumulative as (
# MAGIC     SELECT full_date as event_dt
# MAGIC         , sum(active) over (order by full_date) as active_cum
# MAGIC     FROM raw_dates
# MAGIC     LEFT JOIN
# MAGIC         (SELECT 
# MAGIC             DISTINCT dt as dt
# MAGIC             ,sum(cnt)  active
# MAGIC         FROM 
# MAGIC             ( 
# MAGIC             SELECT dt, 1 as cnt
# MAGIC             FROM 
# MAGIC                 (
# MAGIC                 SELECT DISTINCT 
# MAGIC                     wallet_id, min(cast(event_ts as date)) dt
# MAGIC                 FROM ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns
# MAGIC                 GROUP BY wallet_id
# MAGIC                 ) 
# MAGIC             ORDER BY dt desc
# MAGIC             )
# MAGIC         GROUP BY dt
# MAGIC         ORDER BY dt desc) w
# MAGIC     ON raw_dates.full_date = w.dt
# MAGIC
# MAGIC )
# MAGIC SELECT 
# MAGIC     DISTINCT cast(event_dt as date) as event_dt
# MAGIC     , act.active_cum
# MAGIC     , sig.sign_ups_cum
# MAGIC     , cast(act.active_cum as float)/cast(sig.sign_ups_cum as float) as percenta 
# MAGIC FROM active_cumulative act 
# MAGIC JOIN sign_ups_cumulative_CTE sig 
# MAGIC     ON cast(act.event_dt as date) = cast(sig.marvel_signup_dt as date)
# MAGIC WHERE event_dt >= cast('2021-09-05' as date)
# MAGIC     and event_dt <= current_date()
# MAGIC ORDER BY event_dt
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
# MAGIC
