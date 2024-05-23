# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_star_product_list";

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
# MAGIC -- Daily SP Performance Dashboard Star Product List
# MAGIC     
# MAGIC     with rewards_campaigns as (
# MAGIC         select *, case when length(cmpgn_prod_id) in (8,13) then substring(cmpgn_prod_id,1,(length(cmpgn_prod_id)-1)) else cmpgn_prod_id end as UPC_WS
# MAGIC         from ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_cmpgn_products
# MAGIC         where cmpgn_status_desc in ('ACTIVE', 'EXPIRED')
# MAGIC         and cmpgn_prod_level_nm = 'upc_nbr')
# MAGIC     
# MAGIC     select   distinct
# MAGIC             item.WIN
# MAGIC             ,item.Prime_WIN
# MAGIC             --,item.cons_item_nbr
# MAGIC             ,case when item.cons_item_nbr = 4681141 then item.original_cin else cons_item_nbr end as cons_item_nbr -- added 13/03/2023 by WS to fix issue with deleted CINs (4681141 - ALL_DELETED_ITEMS)
# MAGIC             ,item.upc_nbr
# MAGIC             --,item.dept_nbr
# MAGIC             ,item.dept_desc
# MAGIC             --,item.catg_id
# MAGIC             ,item.catg_desc
# MAGIC             ,initcap(item.vendor_nm) as vendor_nm
# MAGIC             ,prod.UPC_WS as upc
# MAGIC             ,prod.cmpgn_id
# MAGIC             ,camp.cmpgn_nm
# MAGIC             ,camp.cmpgn_start_ts
# MAGIC             ,camp.cmpgn_end_ts
# MAGIC             ,camp.pound_value
# MAGIC             ,camp.activity_type
# MAGIC     from rewards_campaigns as prod
# MAGIC     
# MAGIC     left join
# MAGIC         (select distinct all_links_item_nbr as Prime_WIN, item_nbr as WIN,upc_nbr,cons_item_nbr,original_cin,/*dept_nbr,*/dept_desc,/*catg_id,*/catg_desc,vendor_nm
# MAGIC         from ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy
# MAGIC         where obsolete_dt >= '2022-06-01') as item -- for launch
# MAGIC         on prod.upc_ws = item.upc_nbr
# MAGIC     
# MAGIC     inner join
# MAGIC     ${widget.catalog}.${widget.schema}.vw_cmpgn_setup as camp
# MAGIC     on prod.cmpgn_id = camp.cmpgn_id
# MAGIC     
# MAGIC     where camp.activity_type in ('Super Star Product','Star Product')
# MAGIC     -- to run for full post launch period
# MAGIC     and to_date(camp.cmpgn_start_ts) >= '2022-06-01'
# MAGIC     -- to run for 2022 only
# MAGIC     --and to_date(camp.cmpgn_start_ts) >= '2022-08-01'
# MAGIC     --and to_date(camp.cmpgn_end_ts) <= '2022-12-31'
# MAGIC     -- to run for 2023 only
# MAGIC     --and to_date(camp.cmpgn_end_ts) >= '2023-01-01'
# MAGIC     --and to_date(camp.cmpgn_start_ts) >= '2023-01-01'
# MAGIC     
# MAGIC     -- manual campaign exclusions as required
# MAGIC     and not (item.original_cin = 4681141 and item.cons_item_nbr = 4681141) -- ALL DELETED ITEMS
# MAGIC     and prod.cmpgn_id not in (
# MAGIC     492819 -- 10% Baby Jan 2023 - uses UPCs, Finelines and Dept_Nbrs   
# MAGIC     ,503151 -- 10% George & George Home Event April 2023 - uses UPCs, Finelines and Dept_Nbrs   
# MAGIC     ,505773 -- 10% Pet May 2023 - UPCs & Full Dept, reported separately
# MAGIC     );
# MAGIC
