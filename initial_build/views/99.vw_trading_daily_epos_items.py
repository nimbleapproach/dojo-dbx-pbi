# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_daily_epos_items";

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
# MAGIC                 with Star_Product_List as (
# MAGIC                 select * from ${widget.catalog}.${widget.schema}.vw_trading_star_product_list
# MAGIC                 where activity_type in ('Super Star Product','Star Product'))
# MAGIC                 
# MAGIC                 -- get all Asda tranasction data for these products since start of SSP offers
# MAGIC                 ,Asda_Trans as (
# MAGIC                 select   'All' as chnl_nm
# MAGIC                         ,SPL.activity_type
# MAGIC                         ,SPL.cmpgn_nm
# MAGIC                         ,SPL.cmpgn_id
# MAGIC                         ,tran.visit_dt
# MAGIC                         ,count(distinct tran.basket_id) as customers_proxy
# MAGIC                         ,sum(unmeasured_qty) as items
# MAGIC                         ,round(sum(sale_amt_inc_vat),2) as spend
# MAGIC                 from (select distinct activity_type,cmpgn_nm,cmpgn_id,cons_item_nbr,cmpgn_start_ts,cmpgn_end_ts from Star_Product_List) as SPL
# MAGIC                 left join
# MAGIC                 ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_transaction_item as tran
# MAGIC                 on  tran.cons_item_nbr = SPL.cons_item_nbr
# MAGIC                 and tran.visit_dt between to_date(SPL.cmpgn_start_ts) and date_add(to_date(SPL.cmpgn_end_ts),30)
# MAGIC                 -- full launch
# MAGIC                 where tran.visit_dt between '2022-06-01' and date_add(current_date(),-1)
# MAGIC                 group by 1,2,3,4,5
# MAGIC                 union
# MAGIC                 select   case when tran.channel_src_id = 2 then 'GHS Only' else 'In-store Only' end as chnl_nm
# MAGIC                         ,SPL.activity_type
# MAGIC                         ,SPL.cmpgn_nm
# MAGIC                         ,SPL.cmpgn_id
# MAGIC                         ,tran.visit_dt
# MAGIC                         ,count(distinct tran.basket_id) as customers_proxy
# MAGIC                         ,sum(unmeasured_qty) as items
# MAGIC                         ,round(sum(sale_amt_inc_vat),2) as spend
# MAGIC                 from (select distinct activity_type,cmpgn_nm,cmpgn_id,cons_item_nbr,cmpgn_start_ts,cmpgn_end_ts from Star_Product_List) as SPL
# MAGIC                 left join
# MAGIC                 ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_transaction_item as tran
# MAGIC                 on tran.cons_item_nbr = SPL.cons_item_nbr
# MAGIC                 and tran.visit_dt between to_date(SPL.cmpgn_start_ts) and date_add(to_date(SPL.cmpgn_end_ts),30)
# MAGIC                 -- full launch
# MAGIC                 where tran.visit_dt between '2022-06-01' and date_add(current_date(),-1)
# MAGIC                 group by 1,2,3,4,5)
# MAGIC                 
# MAGIC                 select * from Asda_Trans order by 4,5,1
