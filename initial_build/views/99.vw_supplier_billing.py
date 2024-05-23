# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create - vw_supplier_billing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_supplier_billing";

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

# MAGIC %md
# MAGIC ###Create view query
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW  ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC  select cmpgn_id, 
# MAGIC         cmpgn_nm, 
# MAGIC         offr_type_desc, 
# MAGIC         offr_group_desc, 
# MAGIC         cmpgn_start_ts,
# MAGIC         cmpgn_end_ts, 
# MAGIC         cmpgn_rdmpt_cnt, 
# MAGIC         cmpgn_rdmpt_val, 
# MAGIC         coupon_alloc_cnt, 
# MAGIC         cmpgn_rdmpt_val/100 as coupon_cashpot_given,
# MAGIC         item_nbr, 
# MAGIC         cast(prime_item_nbr as STRING) as prime_item_nbr, 
# MAGIC         prime_item_desc, 
# MAGIC         prime_size_desc, 
# MAGIC         prime_brand_nm, 
# MAGIC         prime_vendor_nbr, 
# MAGIC         prime_vendor_nm, 
# MAGIC         prime_dept_nbr, 
# MAGIC         prime_dept_desc, 
# MAGIC         prime_catg_desc, 
# MAGIC         initcap(prime_product_variant) as prime_product_variant, 
# MAGIC         item_qty, 
# MAGIC         vat_sell_price_amt, 
# MAGIC         scan_rtl_amt, 
# MAGIC         chnl_nm, 
# MAGIC         visit_dt, 
# MAGIC         md_process_id, 
# MAGIC         md_source_ts, 
# MAGIC         md_created_ts, 
# MAGIC         md_source_path, 
# MAGIC         cast(cmpgn_start_ts as date) as cmpgn_start_dt, 
# MAGIC         cast(cmpgn_end_ts as date) as cmpgn_end_ds, 
# MAGIC         null as funding, 
# MAGIC         null as owed
# MAGIC     FROM ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_lylty_supp_bill
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
