# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_sp_cmpgn_details";

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
# MAGIC  -- Asda Rewards Trading Dashboard
# MAGIC                 
# MAGIC                 -- select all Star & Superstar Products from Star Product List
# MAGIC                 with Star_Product_List as (
# MAGIC                 select * from ${widget.catalog}.${widget.schema}.vw_trading_star_product_list
# MAGIC                 where activity_type in ('Super Star Product','Star Product'))
# MAGIC                 
# MAGIC                 -- join all Asda SSP transactions to Asda Rewards transactions
# MAGIC                 ,star_product_dashboard_WS as (
# MAGIC                 select   SPL.activity_type
# MAGIC                         ,initcap(SPL.cmpgn_nm) as cmpgn_nm
# MAGIC                         ,SPL.cmpgn_id
# MAGIC                         --,UPC.UPC as UPC_Number
# MAGIC                         ,0 as UPC_Number
# MAGIC                         ,WIN.Prime_WIN
# MAGIC                         ,to_date(WIN.cmpgn_start_ts) as cmpgn_start_ts
# MAGIC                         ,to_date(WIN.cmpgn_end_ts) as cmpgn_end_ts
# MAGIC                         ,case when SPL.cmpgn_id = 494401 then 'Food To Go' else initcap(WIN.dept_desc) end as dept_desc
# MAGIC                         ,case when SPL.cmpgn_id = 494401 then 'Food for Later' else initcap(WIN.catg_desc) end as catg_desc
# MAGIC                         ,case when SPL.cmpgn_id = 494401 then 'N/A' else initcap(WIN.vendor_nm) end as vendor_nm
# MAGIC                         ,WIN.pound_value
# MAGIC                         ,case when SPL.activity_type = 'Star Product' then '10%' else concat('£',rpad(WIN.pound_value,4,'0')) end as reward_value
# MAGIC                 
# MAGIC                 -- get most appropriate details for each cmpgn_id
# MAGIC                 from
# MAGIC                 (select distinct cmpgn_id, cmpgn_nm, activity_type from Star_Product_List) as SPL
# MAGIC                 
# MAGIC                 inner join
# MAGIC                 (select a.cmpgn_id
# MAGIC                             ,min(coalesce(b.Prime_WIN,c.Prime_WIN)) as prime_WIN
# MAGIC                             --,min(coalesce(b.catg_id,c.catg_id)) as catg_id
# MAGIC                             ,min(coalesce(b.catg_desc,c.catg_desc)) as catg_desc
# MAGIC                             --,min(coalesce(b.dept_nbr,c.dept_nbr)) as dept_nbr
# MAGIC                             ,min(coalesce(b.dept_desc,c.dept_desc)) as dept_desc
# MAGIC                             ,min(coalesce(b.vendor_nm,c.vendor_nm)) as vendor_nm
# MAGIC                             ,min(coalesce(b.cmpgn_start_ts,c.cmpgn_start_ts)) as cmpgn_start_ts
# MAGIC                             ,max(coalesce(b.cmpgn_end_ts,c.cmpgn_end_ts)) as cmpgn_end_ts
# MAGIC                             ,max(coalesce(b.pound_value,c.pound_value)) as pound_value
# MAGIC                     from (select distinct cmpgn_id, Prime_WIN, WIN from Star_Product_List) as a
# MAGIC                     left join Star_Product_List as b -- join on prime WIN where possible
# MAGIC                     on  a.cmpgn_id = b.cmpgn_id
# MAGIC                     and a.Prime_WIN = b.WIN
# MAGIC                     left join Star_Product_List as c -- join on WIN for other cases
# MAGIC                     on  a.cmpgn_id = c.cmpgn_id
# MAGIC                     and a.WIN = c.WIN
# MAGIC                     group by 1) as WIN
# MAGIC                 on SPL.cmpgn_id = WIN.cmpgn_id
# MAGIC                 /*
# MAGIC                 inner join -- pick the most purchased UPC for each campaign ID
# MAGIC                     (select distinct cmpgn_id, UPC from
# MAGIC                             (select  SPL.cmpgn_id
# MAGIC                                     ,SPL.UPC_NBR as UPC
# MAGIC                                     ,row_number() over (partition by cmpgn_id order by sum(unmeasured_qty) desc) as Row_Num
# MAGIC                             from (select distinct cmpgn_id,cons_item_nbr,UPC_nbr from Star_Product_List) as SPL
# MAGIC                             inner join
# MAGIC                             UKProd1_Hive.gb_customer_data_domain_rpt.cdd_rpt_transaction_item as tran
# MAGIC                             on tran.cons_item_nbr = SPL.cons_item_nbr
# MAGIC                             -- full Launch
# MAGIC                             where tran.visit_dt between '2022-08-01' and date_add(current_date(),-1)
# MAGIC                             group by 1,2
# MAGIC                             order by 1,3) as cmpgn_upc
# MAGIC                     where Row_Num = 1) as UPC
# MAGIC                 on WIN.cmpgn_id = UPC.cmpgn_id
# MAGIC                 */)
# MAGIC                 
# MAGIC                 select * from star_product_dashboard_WS order by 3,2,1
# MAGIC                
