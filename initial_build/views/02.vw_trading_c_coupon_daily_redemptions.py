# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_c_coupon_daily_redemptions";

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
# MAGIC select   coup.cmpgn_id
# MAGIC         ,dets.cmpgn_nm
# MAGIC         ,dets.cmpgn_start_ts
# MAGIC         ,dets.cmpgn_end_ts
# MAGIC         ,dets.disc_amt
# MAGIC         ,dets.Supplier
# MAGIC         ,initcap(dets.dept_desc) as Department
# MAGIC         ,dets.catg_desc as Category
# MAGIC         ,dets.Coupon_Type
# MAGIC         ,to_date(coup.coupn_redm_ts) as coupn_redm_dt
# MAGIC         ,count(case when coup.coupn_redm_ts is not null then coup.wallet_id end) as redeemers
# MAGIC         ,sum(coup.rdmpt_cnt) as redemption_count
# MAGIC         ,sum(cast(coup.rdmpt_val as decimal(18,2))/100) as redemption_value
# MAGIC         ,sum(case when upper(wps.chnl_nm) != 'ECOM' then coup.rdmpt_cnt end) as redemptions_store
# MAGIC         ,sum(case when upper(wps.chnl_nm)  = 'ECOM' then coup.rdmpt_cnt end) as redemptions_GHS
# MAGIC         ,sum(case when upper(wps.chnl_nm) != 'ECOM' then cast(coup.rdmpt_val as decimal(18,2))/100 end) as cost_store
# MAGIC         ,sum(case when upper(wps.chnl_nm)  = 'ECOM' then cast(coup.rdmpt_val as decimal(18,2))/100 end) as cost_ghs
# MAGIC from coreprod.gb_mb_dl_tables.coupn_wallets as coup
# MAGIC inner join
# MAGIC (select distinct cmpgn_id, max(cmpgn_nm) as cmpgn_nm, min(cmpgn_start_ts) as cmpgn_start_ts, max(cmpgn_end_ts) as cmpgn_end_ts
# MAGIC                 ,max(disc_amt) as disc_amt, max(Supplier) as Supplier, max(dept_desc) as dept_desc, max(catg_desc) as catg_desc, max(Coupon_Type) as Coupon_Type
# MAGIC  from ${widget.catalog}.${widget.schema}.vw_trading_c_coupon_details group by 1) as dets
# MAGIC  on coup.cmpgn_id = dets.cmpgn_id
# MAGIC  left join
# MAGIC (select distinct wallet_id, event_ts, chnl_nm from coreprod.gb_mb_dl_tables.wallet_pos_txns where to_date(event_ts) >= '2023-10-01')  as wps
# MAGIC  on coup.wallet_id = wps.wallet_id
# MAGIC  and coup.coupn_redm_ts = wps.event_ts
# MAGIC where upper(coup.rec_status_ind) = 'CURRENT' and coup.coupn_redm_ts is not null and to_date(coup.coupn_redm_ts) < current_date()
# MAGIC group by 1,2,3,4,5,6,7,8,9,10
# MAGIC order by 1,6

# COMMAND ----------


