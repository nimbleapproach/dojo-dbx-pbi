# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_active_users_funnel_last_week";

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
# MAGIC with wpt_gap as (
# MAGIC     SELECT 
# MAGIC         wpt.wallet_id
# MAGIC         ,case when (cast(la.regtn_ts as date) < date_add(date_add(current_date(),-7), -37) and la.acct_status_id='TEMP') then 1 else 0
# MAGIC             end as acct_del_ind_1
# MAGIC         , cast(wpt.event_ts as date) as event_ts
# MAGIC         , datediff(cast(wpt.EVENT_TS as date), lead(cast(wpt.event_ts as date),1) over(PARTITION BY wpt.wallet_id order by cast(wpt.event_ts as date) desc)) as diff_last_scan 
# MAGIC         , datediff(cast(wpt.EVENT_TS as date), min(cast(wpt.event_ts as date)) over(PARTITION BY wpt.walleT_id)) as diff_first_scan
# MAGIC         , datediff(date_add(current_date(),-7), min(cast(wpt.event_ts as date)) over(PARTITION BY wpt.walleT_id)) as days_since_first_scan
# MAGIC     FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     JOIN ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC     ON la.wallet_id = wpt.wallet_id
# MAGIC      WHERE wpt.event_ts < date_add(current_date(),-7)
# MAGIC )
# MAGIC
# MAGIC , active_query as (
# MAGIC     SELECT  
# MAGIC          wpt.*
# MAGIC         , ROW_NUMBER() OVER(PARTITION BY wallet_id
# MAGIC                             ORDER BY wpt.event_ts desc) AS row_num
# MAGIC     FROM  wpt_gap wpt
# MAGIC     WHERE wpt.event_ts < date_add(current_date(),-7)--between date_add(current_date(),-7*27) and date_add(current_date(),-8)
# MAGIC     ) 
# MAGIC
# MAGIC , week_totals as (
# MAGIC SELECT --event_ts
# MAGIC     cast(ceil(cast(datediff(date_add(current_date(),-7),event_ts) as float)/7) as int) as weeks_ago
# MAGIC     , acct_del_ind_1
# MAGIC     , sum(total) as total
# MAGIC     , sum(new_one_scanners) as new_one_scanners
# MAGIC     , sum(new_active) as new_active
# MAGIC     , sum(reactive_active) as reactive_active
# MAGIC     , sum(recycling_active) as recycling_active
# MAGIC FROM(
# MAGIC     SELECT wallet_id
# MAGIC             , acct_del_ind_1
# MAGIC             , event_ts 
# MAGIC             , 1 as total
# MAGIC             , case when days_since_first_scan <= 7 then 1 else 0 end as new_one_scanners --include 7 because current_date has no data
# MAGIC             , case when (days_since_first_scan > 7 and diff_first_scan <= 91) then 1 else 0 end as new_active -- new logic so we get the new scanners and everyone else, could maybe work in the scanners on one day to see how many people only try it once?
# MAGIC             , case when diff_first_scan >= 92 and diff_last_scan > 92 then 1 else 0 end as reactive_active --they will be 'reactive' for 91 days
# MAGIC             , case when diff_first_scan >= 92 and diff_last_scan <= 92 then 1 else 0 end as recycling_active  
# MAGIC     FROM active_query
# MAGIC     WHERE row_num = 1
# MAGIC )
# MAGIC GROUP BY weeks_ago, acct_del_ind_1)
# MAGIC
# MAGIC
# MAGIC SELECT week_cat
# MAGIC     , sum(total) as total
# MAGIC     , sum(new_one_scanners) as new_one_scanners
# MAGIC     , sum(new_active) as new_active
# MAGIC     , sum(reactive_active) as reactive_active
# MAGIC     , sum(recycling_active) as recycling_active
# MAGIC FROM(
# MAGIC     SELECT case when weeks_ago < 14 then weeks_ago
# MAGIC             when  weeks_ago >= 14 and acct_del_ind_1 = 0 then'Lapsed' 
# MAGIC             else Null end as week_cat
# MAGIC         , total
# MAGIC         , new_one_scanners
# MAGIC         , new_active
# MAGIC         , reactive_active
# MAGIC         , recycling_active
# MAGIC     FROM week_totals)
# MAGIC WHERE week_cat IS NOT NULL
# MAGIC GROUP BY week_cat
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
