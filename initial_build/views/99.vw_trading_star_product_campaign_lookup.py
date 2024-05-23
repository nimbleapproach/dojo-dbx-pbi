# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_star_product_campaign_lookup";

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
# MAGIC -- Daily SP Performance Dashboard Campaign / Win Lookup check
# MAGIC                         select distinct  activity_type
# MAGIC                                         ,cmpgn_id
# MAGIC                                         ,initcap(cmpgn_nm) as cmpgn_nm
# MAGIC                                         ,0 as dept_nbr
# MAGIC                                         ,initcap(dept_desc) as dept_desc
# MAGIC                                         ,0 as catg_id
# MAGIC                                         ,initcap(catg_desc) as catg_desc
# MAGIC                                         ,initcap(vendor_nm) as vendor_nm
# MAGIC                                         ,CAST(UPC_NBR AS DECIMAL(10,0)) as UPC
# MAGIC                                         ,WIN
# MAGIC                                         ,Prime_WIN
# MAGIC                                         ,Pound_Value
# MAGIC                         from ${widget.catalog}.${widget.schema}.vw_trading_star_product_list
# MAGIC                         where activity_type in ('Super Star Product','Star Product')
# MAGIC                         and upper(cmpgn_nm) not like '%REMOVED%'
# MAGIC                         order by 1 desc,2,3,5,4
