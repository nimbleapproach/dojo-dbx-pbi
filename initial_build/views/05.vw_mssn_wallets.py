# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_mssn_wallets";

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
# MAGIC with dt_lookup as (
# MAGIC     -- For each asda year, find the start and end date and apply a flag = 1 for the current year
# MAGIC     -- This is used to join to mssn_wallets below, to flag all mssn_wallet events in the current YTD   
# MAGIC     SELECT  asda_year, start_dt
# MAGIC         ,coalesce(DATE_ADD(lead(start_dt,1) OVER (ORDER BY asda_year asc ),-1), cast('2099-12-31' as date)) end_dt
# MAGIC         ,CASE WHEN current_date between start_dt and DATE_ADD(lead(start_dt,1) OVER (ORDER BY asda_year asc ),-1) THEN 1 ELSE 0 end as yr_flag
# MAGIC     FROM (
# MAGIC         select  asda_year
# MAGIC                 ,min(day_date) start_dt
# MAGIC         FROM ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
# MAGIC         GROUP BY asda_year
# MAGIC         order by asda_year asc
# MAGIC         )
# MAGIC )
# MAGIC
# MAGIC , mssn_stars as (
# MAGIC     select 
# MAGIC             DISTINCT
# MAGIC             mw.wallet_id
# MAGIC             ,mw.cmpgn_id
# MAGIC             ,cmpgn_seen_ts
# MAGIC             ,cmpgn_cmplt_ts
# MAGIC             -- As of 20221013, we are aware that rdmpt_cnt is wrong. Where the cmpgn is a mission / acca, we force it to 1 (each wallet can complete it once)
# MAGIC             -- For SP we use rdmpt_cnt.
# MAGIC             ,CASE 
# MAGIC                     WHEN cs.activity_type in ( 'ACCA' , 'Mission' ) THEN 1
# MAGIC                     WHEN cs.activity_type in ( 'Star Product' , 'Super Star Product' ) THEN mw.rdmpt_cnt 
# MAGIC                     ELSE  mw.rdmpt_cnt 
# MAGIC                     END AS rdmpt_cnt
# MAGIC
# MAGIC             -- The method by which star product % was calculated changed on 5/19, moving from fixed value to 10% of the sale price.       
# MAGIC             ,CASE 
# MAGIC                 WHEN cs.activity_type = 'Star Product' AND cmpgn_cmplt_ts <= '2022-05-19 00:00:00.000' then cs.pound_value * mw.rdmpt_cnt
# MAGIC                 WHEN cs.activity_type = 'Star Product' AND cmpgn_cmplt_ts > '2022-05-19 00:00:00.000' then mw.rdmpt_val*0.01
# MAGIC                 WHEN cs.activity_type = 'Super Star Product' THEN mw.rdmpt_val * 0.01 * mw.rdmpt_cnt
# MAGIC                 WHEN cs.activity_type = 'Blue Light' THEN mw.rdmpt_val * 0.01 
# MAGIC                 WHEN cs.activity_type = 'Clubs' THEN mw.rdmpt_val * 0.01
# MAGIC                 WHEN cs.activity_type in ( 'ACCA' , 'Mission' ) THEN cs.pound_value
# MAGIC             END AS mission_value_pounds_new
# MAGIC             ,cs.pound_value as mission_value_pounds
# MAGIC             ,store_nbr
# MAGIC             ,rec_status_ind
# MAGIC             ,wpt.chnl_nm
# MAGIC             ,cs.activity_type
# MAGIC             --apply a flag to show if the event was completed in the last week or the one before that
# MAGIC             ,CASE WHEN cast(cmpgn_cmplt_ts as date) BETWEEN DATE_ADD(CURRENT_DATE, -7)  AND CURRENT_DATE THEN  mw.wallet_id ELSE null 
# MAGIC                 END AS cmplt_lst_wk
# MAGIC             ,CASE WHEN cast(cmpgn_cmplt_ts as date) BETWEEN DATE_ADD(CURRENT_DATE, -14)  AND DATE_ADD(CURRENT_DATE,-8) THEN  mw.wallet_id ELSE null 
# MAGIC                 END AS cmplt_prior_wk
# MAGIC     from ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets mw
# MAGIC
# MAGIC     -- Bring in channel name (Store / GHS)
# MAGIC     left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC         on mw.wallet_id = wpt.wallet_id
# MAGIC         and mw.cmpgn_cmplt_ts = cast(wpt.event_ts as timestamp)
# MAGIC     
# MAGIC     --Bring in cmpgn name, etc
# MAGIC     left join ${widget.catalog}.${widget.schema}.vw_cmpgn_setup cs
# MAGIC     on mw.cmpgn_id = cs.cmpgn_id
# MAGIC
# MAGIC     where 1=1
# MAGIC 	and cast(cmpgn_cmplt_ts as date) < CURRENT_DATE
# MAGIC     and cast(cmpgn_cmplt_ts as date) >= cast('2021-09-05' as date)
# MAGIC 	and cmpgn_cmplt_ts is not NULL
# MAGIC     and rec_status_ind='CURRENT'
# MAGIC     and mw.cmpgn_id not in (508017, 533567)
# MAGIC ) 
# MAGIC
# MAGIC -- Had to do blue light seperately due to issue with the rec_status_ind becoming historic for old blue light transactions
# MAGIC , mssn_bl AS (
# MAGIC select 
# MAGIC             DISTINCT
# MAGIC             mw.wallet_id
# MAGIC             ,mw.cmpgn_id
# MAGIC             ,cmpgn_seen_ts
# MAGIC             ,cmpgn_cmplt_ts
# MAGIC             -- As of 20221013, we are aware that rdmpt_cnt is wrong. Where the cmpgn is a mission / acca, we force it to 1 (each wallet can complete it once)
# MAGIC             -- For SP we use rdmpt_cnt.
# MAGIC             ,CASE 
# MAGIC                     WHEN cs.activity_type in ( 'ACCA' , 'Mission' ) THEN 1
# MAGIC                     WHEN cs.activity_type in ( 'Star Product' , 'Super Star Product' ) THEN mw.rdmpt_cnt 
# MAGIC                     ELSE  mw.rdmpt_cnt 
# MAGIC                     END AS rdmpt_cnt
# MAGIC
# MAGIC             -- The method by which star product % was calculated changed on 5/19, moving from fixed value to 10% of the sale price.       
# MAGIC             ,CASE 
# MAGIC                 WHEN cs.activity_type = 'Star Product' AND cmpgn_cmplt_ts <= '2022-05-19 00:00:00.000' then cs.pound_value * mw.rdmpt_cnt
# MAGIC                 WHEN cs.activity_type = 'Star Product' AND cmpgn_cmplt_ts > '2022-05-19 00:00:00.000' then mw.rdmpt_val*0.01
# MAGIC                 WHEN cs.activity_type = 'Super Star Product' THEN mw.rdmpt_val * 0.01 * mw.rdmpt_cnt
# MAGIC                 WHEN cs.activity_type = 'Blue Light' THEN mw.rdmpt_val * 0.01 
# MAGIC                 WHEN cs.activity_type = 'Clubs' THEN mw.rdmpt_val * 0.01
# MAGIC                 WHEN cs.activity_type in ( 'ACCA' , 'Mission' ) THEN cs.pound_value
# MAGIC             END AS mission_value_pounds_new
# MAGIC             ,cs.pound_value as mission_value_pounds
# MAGIC             ,store_nbr
# MAGIC             ,rec_status_ind
# MAGIC             ,wpt.chnl_nm
# MAGIC             ,cs.activity_type
# MAGIC             --apply a flag to show if the event was completed in the last week or the one before that
# MAGIC             ,CASE WHEN cast(cmpgn_cmplt_ts as date) BETWEEN DATE_ADD(CURRENT_DATE, -7)  AND CURRENT_DATE THEN  mw.wallet_id ELSE null 
# MAGIC                 END AS cmplt_lst_wk
# MAGIC             ,CASE WHEN cast(cmpgn_cmplt_ts as date) BETWEEN DATE_ADD(CURRENT_DATE, -14)  AND DATE_ADD(CURRENT_DATE,-8) THEN  mw.wallet_id ELSE null 
# MAGIC                 END AS cmplt_prior_wk
# MAGIC     from ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets mw
# MAGIC
# MAGIC     -- Bring in channel name (Store / GHS)
# MAGIC     left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC         on mw.wallet_id = wpt.wallet_id
# MAGIC         and mw.cmpgn_cmplt_ts = cast(wpt.event_ts as timestamp)
# MAGIC     
# MAGIC     --Bring in cmpgn name, etc
# MAGIC     left join ${widget.catalog}.${widget.schema}.vw_cmpgn_setup cs
# MAGIC     on mw.cmpgn_id = cs.cmpgn_id
# MAGIC
# MAGIC     where 1=1
# MAGIC 	and cast(cmpgn_cmplt_ts as date) < CURRENT_DATE
# MAGIC     and cast(cmpgn_cmplt_ts as date) >= cast('2021-09-05' as date)
# MAGIC 	and cmpgn_cmplt_ts is not NULL
# MAGIC     --and rec_status_ind='CURRENT'
# MAGIC     and mw.cmpgn_id in (508017, 533567)
# MAGIC
# MAGIC )
# MAGIC , mssn_combined AS(
# MAGIC     SELECT * 
# MAGIC     FROM mssn_stars
# MAGIC     UNION ALL
# MAGIC     SELECT *
# MAGIC     FROM mssn_bl
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select mssn_combined.*
# MAGIC         ,dt_lookup.yr_flag as cmplt_ytd 
# MAGIC         --This flag shows is the event happened prior to the last asda week, inclusive (i.e up to and on the most recent friday). 
# MAGIC         -- I believe it can be removed, but am too scared to.
# MAGIC         ,CASE WHEN cast(cmpgn_cmplt_ts as date) <= (select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week) then 1 else 0
# MAGIC             END AS last_asda_wk_flag  
# MAGIC         ,ly.acct_status_id
# MAGIC from mssn_combined
# MAGIC
# MAGIC -- For each record in mssn_wallet, show the relevant flag from the dt_lookup, i.e. = 1 if the event was completed this year
# MAGIC LEFT OUTER JOIN dt_lookup
# MAGIC     ON cast(mssn_combined.cmpgn_cmplt_ts as date) between dt_lookup.start_dt and dt_lookup.end_dt
# MAGIC
# MAGIC -- Bring in account type (temp / perm)
# MAGIC LEFT OUTER JOIN ${widget.catalog}.${widget.schema}.vw_loyalty_acct ly
# MAGIC     ON ly.wallet_id = mssn_combined.wallet_id
# MAGIC     
# MAGIC --Remove test data store (e.g. WMT1)
# MAGIC where try_cast(store_nbr as integer) is not null
# MAGIC ORDER BY cmpgn_cmplt_ts;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
# MAGIC
