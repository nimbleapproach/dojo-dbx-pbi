# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_sp_cmpgn_redeemers";

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
# MAGIC
# MAGIC         -- select all Superstar & Star Products from Star Product List
# MAGIC         with Star_Product_List as (
# MAGIC         select * from ${widget.catalog}.${widget.schema}.vw_trading_star_product_list
# MAGIC         where activity_type in ('Super Star Product','Star Product'))
# MAGIC         
# MAGIC         -- get distinct Asda Rewards customer count by channel for these campaigns since scale launch
# MAGIC         select   case when upper(wps.chnl_nm) = 'ECOM' then 'GHS Only' when upper(wps.chnl_nm) = 'STORE' then 'In-store Only' else '???' end as chnl_nm
# MAGIC                 ,SPL.cmpgn_id
# MAGIC                 ,count(distinct msn.wallet_id) as customers
# MAGIC         from (select distinct cmpgn_id,cmpgn_start_ts,cmpgn_end_ts from Star_Product_List) as SPL
# MAGIC         inner join ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets as msn
# MAGIC         on SPL.cmpgn_id = msn.cmpgn_id
# MAGIC         and msn.cmpgn_cmplt_ts is not null
# MAGIC         and upper(msn.rec_status_ind) = 'CURRENT'
# MAGIC         inner join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns as wps
# MAGIC         on msn.wallet_id = wps.wallet_id
# MAGIC         and cast(msn.event_ts as timestamp) = cast(wps.event_ts as timestamp)
# MAGIC         where to_date(msn.event_ts) between to_date(SPL.cmpgn_start_ts) and to_date(SPL.cmpgn_end_ts)
# MAGIC         and   to_date(msn.event_ts) between '2022-06-01' and date_add(current_date(),-1) -- full launch
# MAGIC         group by 1,2
# MAGIC         union
# MAGIC         select  'All' as chnl_nm
# MAGIC                 ,SPL.cmpgn_id
# MAGIC                 ,count(distinct msn.wallet_id) as customers
# MAGIC         from (select distinct cmpgn_id,cmpgn_start_ts,cmpgn_end_ts from Star_Product_List) as SPL
# MAGIC         inner join ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets as msn
# MAGIC         on SPL.cmpgn_id = msn.cmpgn_id
# MAGIC         and msn.cmpgn_cmplt_ts is not null
# MAGIC         and upper(msn.rec_status_ind) = 'CURRENT'
# MAGIC         where to_date(msn.event_ts) between to_date(SPL.cmpgn_start_ts) and to_date(SPL.cmpgn_end_ts)
# MAGIC         and   to_date(msn.event_ts) between '2022-06-01' and date_add(current_date(),-1) -- full launch
# MAGIC         group by 1,2
# MAGIC         order by 2,1
