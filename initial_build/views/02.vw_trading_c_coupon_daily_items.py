# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_c_coupon_daily_items";

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
# MAGIC select   LAC.cmpgn_id
# MAGIC         ,dets.Supplier
# MAGIC         ,initcap(dets.dept_desc) as Department
# MAGIC         ,dets.catg_desc as Category        
# MAGIC         ,dets.cmpgn_start_ts
# MAGIC         ,dets.cmpgn_end_ts
# MAGIC         ,dets.disc_amt
# MAGIC         ,dets.cmpgn_nm
# MAGIC         ,LAC.item_nbr
# MAGIC         ,LAC.prod_disp_nm
# MAGIC         ,to_date(LAC.coupn_redm_ts) as redemption_date
# MAGIC         ,case when upper(LAC.chnl_nm) = 'ECOM' then 'GHS' else 'In-store' end as chnl_nm
# MAGIC         ,sum(rdmpt_cnt) as redemption_vol
# MAGIC         ,sum(cast(rdmpt_val as decimal(8,2))) as redemption_value
# MAGIC         ,sum(vat_sell_price_amt) as basket_spend
# MAGIC from coreprod.gb_customer_data_domain_odl.cdd_odl_loyalty_adjs_coupon as LAC
# MAGIC inner join
# MAGIC (select distinct cmpgn_id, max(cmpgn_nm) as cmpgn_nm, min(cmpgn_start_ts) as cmpgn_start_ts, max(cmpgn_end_ts) as cmpgn_end_ts
# MAGIC                 ,max(disc_amt) as disc_amt, max(Supplier) as Supplier, max(dept_desc) as dept_desc, max(catg_desc) as catg_desc
# MAGIC  from ${widget.catalog}.${widget.schema}.vw_trading_c_coupon_details where WIN is not null group by 1) as dets
# MAGIC  on LAC.cmpgn_id = dets.cmpgn_id
# MAGIC where LAC.visit_dt >= '2023-10-01' and LAC.supplier_bill_ind = 1 and LAC.in_cmpgn_ind = 1 and LAC.cmpgn_id not in (522461,524842,524843) 
# MAGIC group by 1,2,3,4,5,6,7,8,9,10,11,12
# MAGIC
# MAGIC union
# MAGIC
# MAGIC select   LAC.cmpgn_id
# MAGIC         ,dets.Supplier
# MAGIC         ,initcap(dets.dept_desc) as Department
# MAGIC         ,dets.catg_desc as Category        
# MAGIC         ,dets.cmpgn_start_ts
# MAGIC         ,dets.cmpgn_end_ts
# MAGIC         ,dets.disc_amt
# MAGIC         ,dets.cmpgn_nm
# MAGIC         ,LAC.item_nbr
# MAGIC         ,LAC.prod_disp_nm
# MAGIC         ,to_date(LAC.coupn_redm_ts) as redemption_date
# MAGIC         ,'All' as chnl_nm
# MAGIC         ,sum(rdmpt_cnt) as redemption_vol
# MAGIC         ,sum(cast(rdmpt_val as decimal(8,2))) as redemption_value
# MAGIC         ,sum(vat_sell_price_amt) as basket_spend
# MAGIC from coreprod.gb_customer_data_domain_odl.cdd_odl_loyalty_adjs_coupon as LAC
# MAGIC inner join
# MAGIC (select distinct cmpgn_id, max(cmpgn_nm) as cmpgn_nm, min(cmpgn_start_ts) as cmpgn_start_ts, max(cmpgn_end_ts) as cmpgn_end_ts
# MAGIC                 ,max(disc_amt) as disc_amt, max(Supplier) as Supplier, max(dept_desc) as dept_desc, max(catg_desc) as catg_desc
# MAGIC  from ${widget.catalog}.${widget.schema}.vw_trading_c_coupon_details where WIN is not null group by 1) as dets
# MAGIC  on LAC.cmpgn_id = dets.cmpgn_id
# MAGIC where LAC.visit_dt >= '2023-10-01' and LAC.supplier_bill_ind = 1 and LAC.in_cmpgn_ind = 1 and LAC.cmpgn_id not in (522461,524842,524843) 
# MAGIC group by 1,2,3,4,5,6,7,8,9,10,11,12
# MAGIC order by 1,2,3,4,5,6,7,8,9,10,11,12

# COMMAND ----------


