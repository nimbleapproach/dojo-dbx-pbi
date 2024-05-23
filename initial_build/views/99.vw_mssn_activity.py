# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_mssn_activity";

# COMMAND --------

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

# COMMAND ---------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC with active_customers as (
# MAGIC     SELECT wallet_id
# MAGIC             ,EVENT_TS 
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -7)  AND CURRENT_DATE THEN  1 ELSE 0 END CURRENT_7_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(end_date, -6)  AND end_date THEN  1 ELSE 0 END LAST_WEEK_CURRENT_7_DAY_ACTIVE_FLAG
# MAGIC
# MAGIC     FROM 
# MAGIC             (SELECT WALLET_ID, CAST(EVENT_TS AS DATE) EVENT_TS, 
# MAGIC             (select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week) as end_date
# MAGIC             FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt 
# MAGIC             WHERE 1=1
# MAGIC             --filter out test stores
# MAGIC             AND store_nbr like '^-?\d*$'
# MAGIC             AND CAST(event_ts AS DATE) < CURRENT_DATE
# MAGIC             --only show transactions from the first launch of rewards
# MAGIC             and cast(event_ts as date) >= cast('2021-09-05' as date))
# MAGIC         ) 
# MAGIC
# MAGIC , week_total as (
# MAGIC         -- Total redemption values in the past week across all activities
# MAGIC         SELECT sum(mission_value_pounds_new) as total_sum
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets
# MAGIC         where cmplt_lst_wk !=0
# MAGIC         )
# MAGIC
# MAGIC , last_week_total as (
# MAGIC         SELECT sum(mission_value_pounds_new) as total_sum
# MAGIC             FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets
# MAGIC         where cast(cmpgn_cmplt_ts as date) 
# MAGIC                 BETWEEN DATE_ADD((select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week),-6) 
# MAGIC                 AND (select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC , current_view as (
# MAGIC SELECT week.*, ytd.total_sum as ytd_total_sum, lst_wk.pr_wk
# MAGIC     FROM  (
# MAGIC         -- Idenfity each activity_type, etc
# MAGIC         -- note that we divide by the totals from the above ctes to find percentages
# MAGIC         SELECT activity_type
# MAGIC                 ,sum(mission_value_pounds_new) as rdmpt_val
# MAGIC                 ,sum(mission_value_pounds_new)/(SELECT total_sum from week_total) week_percentage_rdmpt_val
# MAGIC                 ,count(DISTINCT wallet_id) number_unique_completions
# MAGIC                 ,cast(count(DISTINCT wallet_id) as float)/cast((SELECT count(*) FROM active_customers where CURRENT_7_DAY_ACTIVE_FLAG = 1) as float) as percentage_active_customers
# MAGIC
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets b
# MAGIC         where cmplt_lst_wk is not null
# MAGIC         GROUP BY 1 ) week
# MAGIC
# MAGIC     -- Join on to table that shows sum of value by activity in YTD
# MAGIC     -- Note use of cmplt_ytd filter from mssn_wallets
# MAGIC     LEFT OUTER JOIN 
# MAGIC         (SELECT activity_type, sum(mission_value_pounds_new) as total_sum
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets
# MAGIC         where cmplt_ytd !=0
# MAGIC         GROUP BY activity_type) ytd
# MAGIC         ON week.activity_type = ytd.activity_type
# MAGIC
# MAGIC     --Join on to table to show rdmpt_val for prior week
# MAGIC     -- note use of cmplt_prior_wk filter from mssn_wallet
# MAGIC     LEFT OUTER JOIN 
# MAGIC         (SELECT activity_type
# MAGIC             ,sum(mission_value_pounds_new) as pr_wk
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets b
# MAGIC         where cmplt_prior_wk is not null
# MAGIC         GROUP BY 1 ) lst_wk
# MAGIC         ON week.activity_type = lst_wk.activity_type)
# MAGIC
# MAGIC
# MAGIC
# MAGIC , last_friday_view as (
# MAGIC
# MAGIC     SELECT week.*, ytd.total_sum as ytd_total_sum_last_asda_wk, lst_wk.pr_wk as pr_wk_last_asda_wk
# MAGIC     FROM  (
# MAGIC     SELECT activity_type
# MAGIC             ,sum(mission_value_pounds_new) as rdmpt_val_last_asda_wk
# MAGIC             -- ,(SELECT total_sum from week_total) week_total_rdmpt_val
# MAGIC             ,sum(mission_value_pounds_new)/(SELECT total_sum from last_week_total) week_percentage_rdmpt_val_last_asda_wk
# MAGIC             ,count(DISTINCT wallet_id) number_unique_completions_last_asda_wk
# MAGIC             ,(SELECT count(*) FROM active_customers where LAST_WEEK_CURRENT_7_DAY_ACTIVE_FLAG = 1) number_active_customers_last_asda_wk
# MAGIC             ,count(DISTINCT wallet_id) as  number_active_customers_per_area_last_asda_wk
# MAGIC             ,cast(count(DISTINCT wallet_id) as float)/cast((SELECT count(*) FROM active_customers where LAST_WEEK_CURRENT_7_DAY_ACTIVE_FLAG = 1) as float) as percentage_active_customers_last_asda_wk
# MAGIC
# MAGIC     FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets b
# MAGIC     where cast(cmpgn_cmplt_ts as date) 
# MAGIC                 BETWEEN DATE_ADD((select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week),-6) 
# MAGIC                 AND (select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week)
# MAGIC     GROUP BY 1 ) week
# MAGIC
# MAGIC     -- Join on to table that shows sum of value by activity in YTD
# MAGIC     LEFT OUTER JOIN 
# MAGIC         (SELECT activity_type, sum(mission_value_pounds_new) as total_sum
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets
# MAGIC         --all activity in TYD up to last Friday
# MAGIC         where cmplt_ytd !=0
# MAGIC         and last_asda_wk_flag=1
# MAGIC         GROUP BY activity_type) ytd
# MAGIC         ON week.activity_type = ytd.activity_type
# MAGIC
# MAGIC     --Join on to table to show rdmpt_val for  week prior to last friday
# MAGIC     LEFT OUTER JOIN 
# MAGIC         (SELECT activity_type
# MAGIC             ,sum(mission_value_pounds_new) as pr_wk
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_mssn_wallets b
# MAGIC         where cast(cmpgn_cmplt_ts as date) 
# MAGIC                 BETWEEN DATE_ADD((select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week),-6) 
# MAGIC                 AND (select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week)
# MAGIC         GROUP BY 1 ) lst_wk
# MAGIC         ON week.activity_type = lst_wk.activity_type
# MAGIC
# MAGIC )
# MAGIC
# MAGIC select 
# MAGIC     current_view.*
# MAGIC     ,last_friday_view.rdmpt_val_last_asda_wk
# MAGIC     ,last_friday_view.week_percentage_rdmpt_val_last_asda_wk
# MAGIC     ,last_friday_view.number_unique_completions_last_asda_wk
# MAGIC     ,last_friday_view.number_active_customers_last_asda_wk
# MAGIC     ,last_friday_view.number_active_customers_per_area_last_asda_wk
# MAGIC     ,last_friday_view.percentage_active_customers_last_asda_wk
# MAGIC     ,last_friday_view.ytd_total_sum_last_asda_wk
# MAGIC     ,last_friday_view.pr_wk_last_asda_wk
# MAGIC FROM current_view
# MAGIC JOIN last_friday_view
# MAGIC on current_view.activity_type=last_friday_view.activity_type;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
