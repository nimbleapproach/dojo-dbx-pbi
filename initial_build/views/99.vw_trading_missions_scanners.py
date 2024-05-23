# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_missions_scanners";

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
# MAGIC  -- Trading Dashboard Missions summary - scanned wallet volumes
# MAGIC
# MAGIC -- get a list of all missions and their official start and end dates 
# MAGIC with missions as (
# MAGIC select   distinct 
# MAGIC          cmpgn_id
# MAGIC         ,cmpgn_start_ts
# MAGIC         ,cmpgn_end_ts
# MAGIC     --from UserArea."Power BI"."Marvel Supplier Billing"."trading_cmpgn_setup"
# MAGIC     from ${widget.catalog}.${widget.schema}.vw_cmpgn_setup
# MAGIC     where upper(activity_type) in ('ACCA','MISSION')
# MAGIC     and upper(cmpgn_status_desc) in ('ACTIVE', 'EXPIRED')
# MAGIC     -- from launch 
# MAGIC     --and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC
# MAGIC     -- V2 2022 only
# MAGIC     --and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC     --and to_date(cmpgn_end_ts) <= '2022-12-31'
# MAGIC
# MAGIC     -- V3 2022 only
# MAGIC     --and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC     --and to_date(cmpgn_start_ts) <= '2022-12-31'
# MAGIC     
# MAGIC     -- V2 2023 only
# MAGIC     --and to_date(cmpgn_end_ts) >= '2023-01-01'
# MAGIC     -- V3 2023 only
# MAGIC     and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC )
# MAGIC /*
# MAGIC -- find the actual dates the missions were generating events in mssn_wallets
# MAGIC ,MIS_Dates as(
# MAGIC select   MIS.cmpgn_id
# MAGIC         ,min(MSN.event_ts) as cmpgn_start_ts
# MAGIC         ,max(MSN.event_ts) as cmpgn_end_ts
# MAGIC     from missions as MIS
# MAGIC     inner join 
# MAGIC     "UKProd1_Hive"."gb_mb_dl_tables"."mssn_wallets" as MSN
# MAGIC     on  MIS.cmpgn_id = MSN.cmpgn_id
# MAGIC     and upper(MSN.rec_status_ind) = 'CURRENT'
# MAGIC     and MSN.event_ts between MIS.cmpgn_start_ts and MIS.cmpgn_end_ts
# MAGIC group by 1)
# MAGIC */
# MAGIC -- get rewards active user volume during each campaign period, and also for wallets that had the mission 
# MAGIC select   MIS.cmpgn_id
# MAGIC         ,count(distinct POS.wallet_id) as Rewards_Swipers
# MAGIC         ,count(distinct MSN.wallet_id) as Rewards_Swipers_Delivered
# MAGIC --from MIS_Dates as MIS
# MAGIC from missions as MIS
# MAGIC inner join (select distinct wallet_id
# MAGIC                   ,event_ts
# MAGIC                   --,event_dt
# MAGIC --            from ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_wallet_pos_txns
# MAGIC --            where event_dt between '2022-06-01' and date_add(current_date(),-1)) as POS  -- bodge to get it to run WS, 8th Dec 2023
# MAGIC --on  POS.event_dt between to_date(MIS.cmpgn_start_ts) and to_date(MIS.cmpgn_end_ts)
# MAGIC             from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC             where to_date(event_ts) between '2022-11-01' and date_add(current_date(),-1)) as POS  -- launch
# MAGIC on  POS.event_ts between MIS.cmpgn_start_ts and MIS.cmpgn_end_ts
# MAGIC left join (select distinct wallet_id
# MAGIC                  ,cmpgn_id
# MAGIC            from ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets
# MAGIC            where upper(rec_status_ind) = 'CURRENT'
# MAGIC            and cmpgn_seen_ts is not null and to_date(event_ts) >= '2022-06-01') as MSN
# MAGIC on POS.wallet_id = MSN.wallet_id
# MAGIC and MIS.cmpgn_id = MSN.cmpgn_id
# MAGIC group by 1
# MAGIC --order by 1
