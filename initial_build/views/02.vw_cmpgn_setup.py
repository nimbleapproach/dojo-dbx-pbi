# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT database DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_cmpgn_setup";

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
# MAGIC select cs.cmpgn_id
# MAGIC       ,cmpgn_nm
# MAGIC       ,cmpgn_start_ts
# MAGIC       ,cmpgn_end_ts
# MAGIC       ,cs.upd_ts
# MAGIC       ,offr_type_desc
# MAGIC       ,cmpgn_type_nm
# MAGIC       ,cmpgn_tag_array
# MAGIC       ,case when cs.cmpgn_id = 508017 then 'Blue Light'
# MAGIC             when cs.cmpgn_id = 533567 then 'Baby Club'
# MAGIC             when cmpgn_type_nm = 'DISCOUNT_BASKET' then 'Voucher'
# MAGIC             when upper(cmpgn_tag_array) like '%MISSION%' then 'Mission'
# MAGIC             when upper(cmpgn_tag_array) like '%ACCA%' then 'Mission'
# MAGIC             when ((upper(cmpgn_tag_array) like '%STARPRODUCT%' or upper(cmpgn_tag_array) like '%OPENSP%') or (offr_type_desc = 'FIXED_POINTS_PRODUCTS' and cmpgn_type_nm = 'POINTS_FIXED')) then 'Star Product'
# MAGIC        else NULL
# MAGIC        end as category
# MAGIC       ,case when cmpgn_type_nm = 'DISCOUNT_BASKET' then 'Burn'
# MAGIC        else 'Earn'
# MAGIC        end as burn_earn
# MAGIC       ,cmpgn_status_desc
# MAGIC     --   ,reward_pnt_qty
# MAGIC     --   ,disc_amt
# MAGIC       ,case when reward_pnt_qty is NULL then disc_amt / 100.0
# MAGIC             when disc_amt is NULL then reward_pnt_qty / 100.0
# MAGIC        else NULL
# MAGIC        end as pound_value
# MAGIC       ,cmpgn_mode_nm
# MAGIC       ,cmpgn_clsfctn_nm
# MAGIC       ,coupn_exp_type_desc
# MAGIC       ,CASE 
# MAGIC             when upper(cmpgn_tag_array) like '%FREQ%' then 'ACCA' -- this was added by james 19/04/23 to fix issue raised by Joe Rogers
# MAGIC             when upper(cmpgn_tag_array) like '%ACCA%' then 'ACCA'
# MAGIC             WHEN upper(cmpgn_tag_array) like '%MISSION%' then 'Mission'
# MAGIC             WHEN upper(cmpgn_tag_array) like '%SUPERSTAR%' then 'Super Star Product'
# MAGIC             WHEN upper(cmpgn_tag_array) like '%ESSENTIAL_WORKER' then 'Blue Light'
# MAGIC             WHEN upper(cmpgn_tag_array) like '%CLUB_%' then 'Clubs'
# MAGIC             when ((upper(cmpgn_tag_array) like '%STARPRODUCT%' or upper(cmpgn_tag_array) like '%OPENSP%') or (offr_type_desc = 'FIXED_POINTS_PRODUCTS' and cmpgn_type_nm = 'POINTS_FIXED')) then 'Star Product'
# MAGIC         ELSE null
# MAGIC         END AS activity_type
# MAGIC from ${widget.core_catalog}.gb_mb_dl_tables.cmpgn_setup cs
# MAGIC inner join (
# MAGIC     select cmpgn_id, max(upd_ts) as upd_ts
# MAGIC     from ${widget.core_catalog}.gb_mb_dl_tables.cmpgn_setup
# MAGIC     group by cmpgn_id
# MAGIC ) x
# MAGIC on cs.cmpgn_id = x.cmpgn_id
# MAGIC     and cs.upd_ts = x.upd_ts
# MAGIC where ((
# MAGIC     cmpgn_status_desc not in ('DRAFT')
# MAGIC     and cmpgn_nm not like '%TEST%'
# MAGIC     and cast(cmpgn_start_ts as date) <= CURRENT_DATE
# MAGIC     and cast(cmpgn_end_ts as date) >= '2021-10-21'
# MAGIC     and cmpgn_type_nm != 'MANUAL'
# MAGIC ) or (
# MAGIC         cmpgn_type_nm = 'DISCOUNT_BASKET'
# MAGIC         and cmpgn_nm = 'Asda Rewards Voucher'
# MAGIC         and cast(cmpgn_end_ts as date) >= '2031-12-31'
# MAGIC     ))
# MAGIC     AND rec_status_ind = 'CURRENT'
# MAGIC order by cmpgn_id, upd_ts desc;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
