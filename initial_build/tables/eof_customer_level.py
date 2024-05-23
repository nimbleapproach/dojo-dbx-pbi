# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "eof_customer_level";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
spark.conf.set ('widget.table_name', dbutils.widgets.get("table_name"))

#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.text('table_name', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS database,
# MAGIC        '${widget.table_name}' AS table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS ${widget.catalog}.${widget.schema}.${widget.table_name} (
# MAGIC       wallet_id BIGINT, 
# MAGIC       shopped_weeks BIGINT,     
# MAGIC       total_visits BIGINT, 
# MAGIC       total_sales DOUBLE, 
# MAGIC       total_units DOUBLE,
# MAGIC       ar_sales DOUBLE, 
# MAGIC       ar_units DOUBLE,
# MAGIC       ar_visits BIGINT, 
# MAGIC       earn DOUBLE, 
# MAGIC       first_ar_visit BIGINT, 
# MAGIC       active_1d BIGINT,
# MAGIC       active_7d BIGINT, 
# MAGIC       active_30d BIGINT,  
# MAGIC       active_13wk BIGINT,    
# MAGIC       fv_cross_shop_1d BIGINT, 
# MAGIC       fv_cross_shop_7d BIGINT, 
# MAGIC       fv_cross_shop_30d BIGINT, 
# MAGIC       fv_cross_shop_13wk BIGINT,  
# MAGIC       cust_cross_shop_1d BIGINT, 
# MAGIC       cust_cross_shop_7d BIGINT, 
# MAGIC       cust_cross_shop_30d BIGINT, 
# MAGIC       cust_cross_shop_13wk BIGINT 
# MAGIC     ) 
# MAGIC      COMMENT "This table contains something"
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
