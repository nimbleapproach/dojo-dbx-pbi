# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_missions_summary";

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
# MAGIC with missions as (
# MAGIC             select   distinct
# MAGIC                     cs1.cmpgn_id
# MAGIC                     ,cs1.cmpgn_nm
# MAGIC                     ,to_date(cs1.cmpgn_start_ts) as cmpgn_start_dt
# MAGIC                     ,to_date(cs1.cmpgn_end_ts) as cmpgn_end_dt
# MAGIC                     ,cs1.activity_type
# MAGIC                 -- ,offr_type_desc
# MAGIC                     ,cs1.pound_value
# MAGIC                     ,cs2.trans_needed_reward_pnt_qty
# MAGIC                     --,cs2.trans_needed_unit_qty
# MAGIC                 --from UserArea."Power BI"."Marvel Supplier Billing"."trading_cmpgn_setup" as cs1
# MAGIC                 from ${widget.catalog}.${widget.schema}.vw_cmpgn_setup as cs1
# MAGIC                 left join
# MAGIC                     (select cmpgn_id, max(trans_needed_reward_pnt_qty)/100 as trans_needed_reward_pnt_qty--, max(trans_needed_unit_qty) as trans_needed_unit_qty
# MAGIC                     from  ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_cmpgn_setup group by 1) as cs2
# MAGIC                 on cs1.cmpgn_id = cs2.cmpgn_id
# MAGIC                 where upper(cs1.activity_type) in ('ACCA','MISSION')
# MAGIC                 and upper(cs1.cmpgn_status_desc) in ('ACTIVE', 'EXPIRED')
# MAGIC                 -- full launch
# MAGIC                 --and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC                 
# MAGIC                 -- V2 2022 only
# MAGIC                 --and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC                 --and to_date(cmpgn_end_ts) <= '2022-12-31'
# MAGIC             
# MAGIC                 -- V3 2022 only
# MAGIC                 --and to_date(cmpgn_start_ts) >= '2022-06-01'
# MAGIC                 --and to_date(cmpgn_start_ts) <= '2022-12-31'  
# MAGIC                 
# MAGIC                 -- V2 2023 only
# MAGIC                 --and to_date(cs1.cmpgn_end_ts) >= '2023-01-01'
# MAGIC                 -- V3 2023 only
# MAGIC                 and to_date(cs1.cmpgn_start_ts) >= '2022-06-01'    
# MAGIC             )    
# MAGIC             
# MAGIC             select   mis.cmpgn_id
# MAGIC                     ,mis.cmpgn_nm
# MAGIC                     ,case when mis.activity_type = 'ACCA' then 'Accumulator' else mis.activity_type end as activity_type
# MAGIC                     --,case when mis.offr_type_desc = 'FIXED_POINTS_BASKET' then 'Fixed Points - Basket'
# MAGIC                     --      when mis.offr_type_desc = 'FIXED_POINTS_PRODUCTS' then 'Fixed Points - Products'
# MAGIC                     --      else initcap(mis.offr_type_desc) end as offr_type_desc
# MAGIC                     ,'Continuity' as offr_type_desc
# MAGIC                     ,mis.pound_value
# MAGIC                     ,mis.trans_needed_reward_pnt_qty
# MAGIC                     --,mis.trans_needed_unit_qty
# MAGIC                     ,mis.cmpgn_start_dt
# MAGIC                     ,mis.cmpgn_end_dt
# MAGIC             --        ,mis.cmpgn_start_dt as event_ts
# MAGIC                     ,to_date(msn.event_ts) as event_ts
# MAGIC                     ,case when cmpgn_end_dt >= current_date() then 'Active' else 'Expired' end as Mission_Status
# MAGIC                     ,count(distinct case when msn.cmpgn_seen_ts is not null then msn.wallet_id end) as customers_seen
# MAGIC                     ,count(distinct case when msn.cmpgn_seen_ts is not null and wpt.wallet_id is not null then msn.wallet_id end) as customers_progressed
# MAGIC                     ,0 as customers_seen_not_cmplt
# MAGIC                     --,count(distinct case when msn.cmpgn_seen_ts is not null and msn.cmpgn_cmplt_ts is null then msn.wallet_id end) as customers_seen_not_cmplt
# MAGIC                     ,count(distinct case when msn.cmpgn_seen_ts is not null and msn.cmpgn_cmplt_ts is not null then msn.wallet_id end) as redeemers
# MAGIC                     ,count(distinct case when msn.cmpgn_seen_ts is not null and msn.cmpgn_cmplt_ts is not null then msn.wallet_id end)*mis.pound_value as cashpot_earn
# MAGIC                     --,round(sum(case when msn.cmpgn_cmplt_ts is not null then mis.pound_value end),2) as cashpot_earn
# MAGIC                     --,sum(msn.rdmpt_cnt) as redeemers
# MAGIC                     --,round(sum(msn.rdmpt_cnt)*mis.pound_value,2) as cashpot_earn   
# MAGIC                 from missions as mis
# MAGIC                 left join
# MAGIC                 ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets as msn
# MAGIC                     on mis.cmpgn_id = msn.cmpgn_id
# MAGIC                 left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC                     on msn.wallet_id = wpt.wallet_id
# MAGIC                     and cast(msn.event_ts as timestamp) = cast(wpt.event_ts as timestamp)
# MAGIC                 where upper(msn.rec_status_ind) = 'CURRENT'
# MAGIC                 and to_date(msn.event_ts) < current_date()
# MAGIC                 and to_date(msn.event_ts) >= '2022-06-01' -- full launch
# MAGIC                 --and (regexp_matches(wpt.store_nbr, '^-?\d*$') is true or wpt.store_nbr is null)
# MAGIC             group by 1,2,3,4,5,6,7,8,9
# MAGIC             --order by 1 
