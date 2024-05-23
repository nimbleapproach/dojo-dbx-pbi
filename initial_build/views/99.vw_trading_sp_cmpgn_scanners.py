# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_sp_cmpgn_scanners";

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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC -- select all Super Star Products from Star Product List
# MAGIC         with Star_Product_List as (
# MAGIC         select distinct
# MAGIC                 cmpgn_id
# MAGIC                 ,cmpgn_start_ts
# MAGIC                 ,cmpgn_end_ts
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_trading_star_product_list
# MAGIC         where activity_type in ('Super Star Product','Star Product'))
# MAGIC         --order by 1)
# MAGIC         
# MAGIC         -- Rewards Customer Penetration calculation switched off 6th Nov 2023 by WS for efficiency
# MAGIC         select   SPL.cmpgn_id
# MAGIC                 ,'All' as chnl_nm
# MAGIC                 ,0 as Rewards_Swipers
# MAGIC         from Star_Product_List as SPL
# MAGIC         group by 1
# MAGIC         order by 1
# MAGIC         
# MAGIC         /*
# MAGIC         ,SPL_Dates as(
# MAGIC         select   SPL.cmpgn_id
# MAGIC                 ,min(msn.event_ts) as cmpgn_start_ts
# MAGIC                 ,max(msn.event_ts) as cmpgn_end_ts
# MAGIC         from Star_Product_List as SPL
# MAGIC         inner join
# MAGIC         "UKProd1_Hive"."gb_mb_dl_tables"."mssn_wallets" as msn
# MAGIC         on  SPL.cmpgn_id = msn.cmpgn_id
# MAGIC         and upper(msn.rec_status_ind) = 'CURRENT'
# MAGIC         and to_date(msn.cmpgn_cmplt_ts) >= '2022-11-01' -- full launch
# MAGIC         and msn.event_ts between spl.cmpgn_start_ts and spl.cmpgn_end_ts
# MAGIC         group by 1)
# MAGIC         */
# MAGIC         /*
# MAGIC         -- get rewards active user volume during each campaign period
# MAGIC         select   SPL.cmpgn_id
# MAGIC                 ,'All' as chnl_nm
# MAGIC                 ,count(distinct pos.wallet_id) as Rewards_Swipers
# MAGIC         from
# MAGIC         
# MAGIC         --Star_Product_List as SPL
# MAGIC         
# MAGIC         (select distinct cmpgn_id
# MAGIC                 ,cmpgn_start_ts
# MAGIC                 ,cmpgn_end_ts
# MAGIC         --from SPL_Dates) as SPL
# MAGIC         from Star_Product_List) as SPL
# MAGIC         */    
# MAGIC         /*
# MAGIC         inner join (select distinct
# MAGIC                         wallet_id
# MAGIC                         ,chnl_nm
# MAGIC                         ,event_ts
# MAGIC                 from "UKProd1_Hive"."gb_mb_dl_tables"."wallet_pos_txns"
# MAGIC                 where to_date(event_ts) between '2022-11-01' and date_add(current_date(),-1)) as pos -- full launch
# MAGIC         on pos.event_ts between SPL.cmpgn_start_ts and SPL.cmpgn_end_ts
# MAGIC         group by 1,2
# MAGIC         /*
# MAGIC         union
# MAGIC         select   SPL.cmpgn_id
# MAGIC                 ,case when upper(chnl_nm) = 'ECOM' then 'GHS Only' when upper(chnl_nm) = 'STORE' then 'In-store Only' else '???' end as chnl_nm
# MAGIC                 ,count(distinct pos.wallet_id) as Rewards_Swipers
# MAGIC         from
# MAGIC         
# MAGIC         Star_Product_List as SPL
# MAGIC         /*
# MAGIC         (select distinct cmpgn_id
# MAGIC                 ,cmpgn_start_ts
# MAGIC                 ,cmpgn_end_ts
# MAGIC         from SPL_Dates) as SPL
# MAGIC         /*      
# MAGIC         inner join (select distinct
# MAGIC                         wallet_id
# MAGIC                         ,chnl_nm
# MAGIC                         ,event_ts
# MAGIC                 from "UKProd1_Hive"."gb_mb_dl_tables"."wallet_pos_txns"
# MAGIC                 where to_date(event_ts) between '2022-08-01' and date_add(current_date(),-1)) as pos -- full launch
# MAGIC         on pos.event_ts between SPL.cmpgn_start_ts and SPL.cmpgn_end_ts
# MAGIC         group by 1,2
# MAGIC         */
# MAGIC
# MAGIC         -- order by 1,2
# MAGIC         */
# MAGIC         */
# MAGIC         */
# MAGIC
