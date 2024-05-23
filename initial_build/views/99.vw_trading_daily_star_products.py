# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_daily_star_products";

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
# MAGIC  -- select all Super Star Products from Star Product List
# MAGIC                     with Star_Product_List as (
# MAGIC                     select * from ${widget.catalog}.${widget.schema}.vw_trading_star_product_list
# MAGIC                     where activity_type in ('Super Star Product','Star Product'))
# MAGIC                     
# MAGIC                     -- get all Asda Rewards transaction data for these products since start of SSP offers
# MAGIC                     --,Rewards_Trans as (
# MAGIC                     select   case when upper(chnl_nm) = 'ECOM' then 'GHS Only' when upper(chnl_nm) = 'STORE' then 'In-store Only' else '???' end as chnl_nm
# MAGIC                             ,SPL.activity_type
# MAGIC                             ,SPL.cmpgn_nm
# MAGIC                             ,SPL.cmpgn_id
# MAGIC                             ,to_date(msn.event_ts) as visit_dt
# MAGIC                             ,count(distinct msn.wallet_id) as customers
# MAGIC                             --,max(Swipers.Rewards_Swipers) as Rewards_Swipers
# MAGIC                             ,count(distinct case when to_date(msn.event_ts) = msn.first_swipe then msn.wallet_id end) as first_swipe_customers
# MAGIC                             ,sum(rdmpt_cnt) as items
# MAGIC                             ,sum(case when SPL.activity_type = 'Star Product' and msn.cmpgn_cmplt_ts <='2022-05-19 00:00:00.000' then SPL.pound_value * msn.rdmpt_cnt
# MAGIC                                 when SPL.activity_type = 'Star Product' and msn.cmpgn_cmplt_ts > '2022-05-19 00:00:00.000' then msn.rdmpt_val*0.01
# MAGIC                                 when SPL.activity_type = 'Super Star Product' then msn.rdmpt_val * 0.01 * msn.rdmpt_cnt
# MAGIC                                 when SPL.activity_type in ( 'ACCA' , 'Mission' ) then SPL.pound_value end) as cashpot3
# MAGIC                     from (select distinct activity_type,cmpgn_nm,cmpgn_id,pound_value from Star_Product_List) as SPL
# MAGIC                     left join
# MAGIC                         (select wal.*,subq.first_swipe
# MAGIC                         from ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets as wal
# MAGIC                         left join (select wallet_id, to_date(min(event_ts)) as first_swipe
# MAGIC                                     from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns group by 1) as subq
# MAGIC                         on wal.wallet_id = subq.wallet_id) as msn
# MAGIC                     on SPL.cmpgn_id = msn.cmpgn_id
# MAGIC                     and msn.cmpgn_cmplt_ts is not null
# MAGIC                     and upper(msn.rec_status_ind) = 'CURRENT'
# MAGIC                     inner join (select distinct wallet_id, chnl_nm, event_ts from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns) as wps
# MAGIC                     on  msn.wallet_id = wps.wallet_id
# MAGIC                     and cast(msn.event_ts as timestamp) = cast(wps.event_ts as timestamp)
# MAGIC                     /*
# MAGIC                     inner join
# MAGIC                         (select  to_date(event_ts) as swipe_date
# MAGIC                                 ,chnl_nm
# MAGIC                                 ,count(distinct wallet_id) as Rewards_Swipers
# MAGIC                         from "UKProd1_Hive"."gb_mb_dl_tables"."wallet_pos_txns" group by 1,2) as Swipers
# MAGIC                     on to_date(msn.event_ts) = Swipers.swipe_date
# MAGIC                     and wps.chnl_nm = Swipers.chnl_nm
# MAGIC                     */
# MAGIC                     -- full launch
# MAGIC                     where to_date(msn.event_ts) between '2022-06-01' and date_add(current_date(),-1)
# MAGIC                     group by 1,2,3,4,5
# MAGIC                     having customers > 0 and visit_dt < current_date()
# MAGIC                     union
# MAGIC                     select   'All' as chnl_nm
# MAGIC                             ,SPL.activity_type
# MAGIC                             ,SPL.cmpgn_nm
# MAGIC                             ,SPL.cmpgn_id
# MAGIC                             ,to_date(msn.event_ts) as visit_dt
# MAGIC                             ,count(distinct msn.wallet_id) as customers
# MAGIC                             --,max(Swipers.Rewards_Swipers) as Rewards_Swipers
# MAGIC                             ,count(distinct case when to_date(msn.event_ts) = msn.first_swipe then msn.wallet_id end) as first_swipe_customers
# MAGIC                             ,sum(rdmpt_cnt) as items
# MAGIC                             ,sum(case when SPL.activity_type = 'Star Product' and msn.cmpgn_cmplt_ts <='2022-05-19 00:00:00.000' then SPL.pound_value * msn.rdmpt_cnt
# MAGIC                                 when SPL.activity_type = 'Star Product' and msn.cmpgn_cmplt_ts > '2022-05-19 00:00:00.000' then msn.rdmpt_val*0.01
# MAGIC                                 when SPL.activity_type = 'Super Star Product' then msn.rdmpt_val * 0.01 * msn.rdmpt_cnt
# MAGIC                                 when SPL.activity_type in ( 'ACCA' , 'Mission' ) then SPL.pound_value end) as cashpot3
# MAGIC                     from (select distinct activity_type,cmpgn_nm,cmpgn_id,pound_value from Star_Product_List) as SPL
# MAGIC                     left join
# MAGIC                         (select wal.*,subq.first_swipe
# MAGIC                         from ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets as wal
# MAGIC                         left join (select wallet_id, to_date(min(event_ts)) as first_swipe
# MAGIC                                     from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns group by 1) as subq
# MAGIC                         on wal.wallet_id = subq.wallet_id) as msn
# MAGIC                     on SPL.cmpgn_id = msn.cmpgn_id
# MAGIC                     and msn.cmpgn_cmplt_ts is not null
# MAGIC                     and upper(msn.rec_status_ind) = 'CURRENT'
# MAGIC                     inner join (select distinct wallet_id, chnl_nm, event_ts from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns) as wps
# MAGIC                     on  msn.wallet_id = wps.wallet_id
# MAGIC                     and cast(msn.event_ts as timestamp) = cast(wps.event_ts as timestamp)
# MAGIC                     /*
# MAGIC                     inner join
# MAGIC                         (select  to_date(event_ts) as swipe_date,count(distinct wallet_id) as Rewards_Swipers
# MAGIC                         from "UKProd1_Hive"."gb_mb_dl_tables"."wallet_pos_txns" group by 1) as Swipers
# MAGIC                     on to_date(msn.event_ts) = Swipers.swipe_date
# MAGIC                     */
# MAGIC                     -- full launch
# MAGIC                     where to_date(msn.event_ts) between '2022-06-01' and date_add(current_date(),-1)
# MAGIC                     group by 1,2,3,4,5
# MAGIC                     having customers > 0 and visit_dt < current_date()
# MAGIC                     --)
# MAGIC                     order by 4,5,1
# MAGIC                     
# MAGIC                     --select * from Rewards_Trans order by 4,5,1
# MAGIC                
