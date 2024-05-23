# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_c_coupon_details";

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
# MAGIC with coupons as(
# MAGIC select   distinct 
# MAGIC          cmpgn_tag_array
# MAGIC         ,cmpgn_id
# MAGIC         ,cmpgn_nm
# MAGIC         ,cmpgn_start_ts
# MAGIC         ,cmpgn_end_ts
# MAGIC         ,cmpgn_status_desc
# MAGIC         ,disc_amt
# MAGIC from coreprod.gb_customer_data_domain_odl.cdd_odl_cmpgn_setup 
# MAGIC where upper(cmpgn_tag_array) like '%COUPON%' and to_date(cmpgn_start_ts) >= '2023-10-01')
# MAGIC /*
# MAGIC ,campaigns as (
# MAGIC     select *, case when length(cmpgn_prod_id) in (8,13) then substring(cmpgn_prod_id,1,(length(cmpgn_prod_id)-1)) else cmpgn_prod_id end as UPC_WS
# MAGIC     from coreprod.gb_customer_data_domain_odl.cdd_odl_cmpgn_products
# MAGIC     where cmpgn_status_desc in ('ACTIVE', 'EXPIRED') 
# MAGIC     and cmpgn_prod_level_nm = 'upc_nbr')
# MAGIC */
# MAGIC ,campaigns as (
# MAGIC     select *, case when cmpgn_prod_level_nm = 'upc_nbr' and length(cmpgn_prod_id) in (8,13) then substring(cmpgn_prod_id,1,(length(cmpgn_prod_id)-1)) 
# MAGIC                    when cmpgn_prod_level_nm = 'upc_nbr' then cmpgn_prod_id 
# MAGIC                    else null end as UPC_WS
# MAGIC     from coreprod.gb_customer_data_domain_odl.cdd_odl_cmpgn_products
# MAGIC     where cmpgn_status_desc in ('ACTIVE', 'EXPIRED'))
# MAGIC     
# MAGIC select   distinct 
# MAGIC          coup.cmpgn_tag_array
# MAGIC         ,prod.cmpgn_id
# MAGIC         ,coup.cmpgn_nm
# MAGIC         ,case when hour(coup.cmpgn_start_ts) = 23 then timestampadd(hour,1,coup.cmpgn_start_ts) else coup.cmpgn_start_ts end as cmpgn_start_ts
# MAGIC         ,coup.cmpgn_end_ts
# MAGIC         ,coup.disc_amt/100 as disc_amt
# MAGIC         ,prod.UPC_WS as upc     
# MAGIC         ,cast(item.upc_nbr as varchar(15)) as upc_nbr 
# MAGIC         ,item.WIN
# MAGIC         ,case when item.cons_item_nbr = 4681141 then item.original_cin else cons_item_nbr end as cons_item_nbr 
# MAGIC             -- added 13/03/2023 by WS to fix issue with deleted CINs (4681141 - ALL_DELETED_ITEMS)
# MAGIC         ,item.Prime_WIN
# MAGIC         ,initcap(item.dept_desc) as dept_desc
# MAGIC         ,item.catg_desc
# MAGIC         ,initcap(item.vendor_nm) as Supplier   
# MAGIC         ,case when xmas.Coupon_Type is not null then xmas.Coupon_Type 
# MAGIC               when HTFeb24.cmpgn_id is not null then 'Home + Toys Game Feb 2024'
# MAGIC               else 'Product Coupon' end as Coupon_Type
# MAGIC
# MAGIC from campaigns as prod
# MAGIC
# MAGIC inner join
# MAGIC coupons coup
# MAGIC on prod.cmpgn_id = coup.cmpgn_id
# MAGIC
# MAGIC left join 
# MAGIC (
# MAGIC   select * 
# MAGIC   from (
# MAGIC     values
# MAGIC       (527534, 'Xmas Game'),
# MAGIC       (527535,	'Xmas Game'),
# MAGIC       (527536,	'Xmas Game'),
# MAGIC       (527537,	'Xmas Game'),
# MAGIC       (527538,	'Xmas Game'),
# MAGIC       (527539,	'Xmas Game'),
# MAGIC       (527548,	'Xmas Game'),
# MAGIC       (527554,	'Xmas Game'),
# MAGIC       (527559,	'Xmas Game'),
# MAGIC       (527560,	'Xmas Game'),
# MAGIC       (527563,	'Xmas Game'),
# MAGIC       (527776,	'Xmas Game'),
# MAGIC       (527601,	'Xmas Game'),
# MAGIC       (527564,	'Xmas Game'),
# MAGIC       (527728,	'Xmas Game'),
# MAGIC       (527600,	'Xmas Game'),
# MAGIC       (527760,	'Xmas Game'),
# MAGIC       (527727,	'Xmas Game'),
# MAGIC       (527596,	'Xmas Game'),
# MAGIC       (527565,	'Xmas Game'),
# MAGIC       (527566,	'Xmas Game'),
# MAGIC       (528259,	'Xmas Coupon'),
# MAGIC       (528000,	'Xmas Coupon'),
# MAGIC       (527847,	'Xmas Coupon'),
# MAGIC       (528062,	'Xmas Coupon'),
# MAGIC       (528076,	'Xmas Coupon'),
# MAGIC       (528077,	'Xmas Coupon'),
# MAGIC       (528198,	'Xmas Coupon'),
# MAGIC       (528241,	'Xmas Coupon'),
# MAGIC       (528247,	'Xmas Coupon'),
# MAGIC       (528248,	'Xmas Coupon'),
# MAGIC       (528249,	'Xmas Coupon'),
# MAGIC       (528250,	'Xmas Coupon'),
# MAGIC       (528251,	'Xmas Coupon'),
# MAGIC       (528227,	'Xmas Coupon'),
# MAGIC       (528252,	'Xmas Coupon'),
# MAGIC       (528414,	'Xmas Coupon'),
# MAGIC       (528415,	'Xmas Coupon'),
# MAGIC       (528416,	'Xmas Coupon'),
# MAGIC       (528420,	'Xmas Coupon'),
# MAGIC       (528424,	'Xmas Coupon'),
# MAGIC       (528426,	'Xmas Coupon'),
# MAGIC       (528427,	'Xmas Coupon'),
# MAGIC       (528429,	'Xmas Coupon'),
# MAGIC       (528430,	'Xmas Coupon'),
# MAGIC       (528283,	'Xmas Coupon'),
# MAGIC       (528296,	'Xmas Coupon'),
# MAGIC       (528299,	'Xmas Coupon'),
# MAGIC       (528301,	'Xmas Coupon'),
# MAGIC       (528306,	'Xmas Coupon'),
# MAGIC       (528308,	'Xmas Coupon'),
# MAGIC       (528311,	'Xmas Coupon'),
# MAGIC       (528317,	'Xmas Coupon'),
# MAGIC       (528353,	'Xmas Coupon'),
# MAGIC       (528357,	'Xmas Coupon'),
# MAGIC       (528388,	'Xmas Coupon'),
# MAGIC       (528394,	'Xmas Coupon'),
# MAGIC       (528396,	'Xmas Coupon'),
# MAGIC       (528400,	'Xmas Coupon'),
# MAGIC       (528401,	'Xmas Coupon'),
# MAGIC       (528402,	'Xmas Coupon'),
# MAGIC       (528403,	'Xmas Coupon'),
# MAGIC       (528484,	'Xmas Coupon'),
# MAGIC       (528256,	'Xmas Coupon'),
# MAGIC       (528260,	'Xmas Coupon'),
# MAGIC       (528261,	'Xmas Coupon'),
# MAGIC       (528262,	'Xmas Coupon'),
# MAGIC       (528263,	'Xmas Coupon'),
# MAGIC       (528264,	'Xmas Coupon'),
# MAGIC       (528265,	'Xmas Coupon'),
# MAGIC       (528266,	'Xmas Coupon'),
# MAGIC       (528267,	'Xmas Coupon'),
# MAGIC       (528268,	'Xmas Coupon'),
# MAGIC       (528276,	'Xmas Coupon'),
# MAGIC       (528270,	'Xmas Coupon'),
# MAGIC       (528272,	'Xmas Coupon'),
# MAGIC       (528273,	'Xmas Coupon'),
# MAGIC       (528278,	'Xmas Coupon'),
# MAGIC       (528282,	'Xmas Coupon'),
# MAGIC       (528274,	'Xmas Coupon'),
# MAGIC       (528298,	'Xmas Coupon'),
# MAGIC       (528405,	'Xmas Coupon'),
# MAGIC       (528407,	'Xmas Coupon'),
# MAGIC       (528408,	'Xmas Coupon'),
# MAGIC       (528410,	'Xmas Coupon'),
# MAGIC       (528411,	'Xmas Coupon'),
# MAGIC       (528412,	'Xmas Coupon'),
# MAGIC       (528425,	'Xmas Coupon'),
# MAGIC       (528447,	'Xmas Coupon'),
# MAGIC       (528449,	'Xmas Coupon'),
# MAGIC       (528450,	'Xmas Coupon')
# MAGIC
# MAGIC   ) as temp_table (cmpgn_id, Coupon_Type)
# MAGIC   )
# MAGIC as xmas
# MAGIC on prod.cmpgn_id = xmas.cmpgn_id
# MAGIC
# MAGIC left join 
# MAGIC (
# MAGIC   select * 
# MAGIC   from (
# MAGIC     values
# MAGIC       (531111),
# MAGIC       (531112),
# MAGIC       (531113),
# MAGIC       (531116),
# MAGIC       (531118),
# MAGIC       (531119),
# MAGIC       (531120),
# MAGIC       (531122),
# MAGIC       (531124),
# MAGIC       (531125),
# MAGIC       (531127),
# MAGIC       (531129),
# MAGIC       (531131),
# MAGIC       (531132),
# MAGIC       (531133),
# MAGIC       (531732)
# MAGIC   ) as temp_table2 (cmpgn_id)
# MAGIC )
# MAGIC as HTFeb24
# MAGIC on prod.cmpgn_id = HTFeb24.cmpgn_id
# MAGIC
# MAGIC left join
# MAGIC      (select distinct all_links_item_nbr as Prime_WIN, item_nbr as WIN,upc_nbr,cons_item_nbr,original_cin,dept_desc,catg_desc,vendor_nm
# MAGIC       from coreprod.gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy 
# MAGIC       where obsolete_dt >= '2022-08-01') as item -- for launch
# MAGIC on prod.upc_ws = item.upc_nbr   

# COMMAND ----------


