# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "crm_send_level_engine";

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
# MAGIC  CREATE TABLE IF NOT EXISTS ${widget.catalog}.${widget.schema}.${widget.table_name} 
# MAGIC  (
# MAGIC     type STRING,
# MAGIC     days_analysed INT,
# MAGIC     journey_id INT,
# MAGIC     journey_name STRING,
# MAGIC     groups STRING,
# MAGIC     journey_date DATE,
# MAGIC     customers BIGINT,
# MAGIC     george_orderers BIGINT,
# MAGIC     george_orders BIGINT,
# MAGIC     george_items BIGINT,
# MAGIC     george_sales DECIMAL(19,2),
# MAGIC     ghs_orderers BIGINT,
# MAGIC     ghs_orders BIGINT,
# MAGIC     ghs_items BIGINT,
# MAGIC     ghs_sales DECIMAL(16,2),
# MAGIC     ghs_orderers_mailed DOUBLE,
# MAGIC     george_orderers_mailed DOUBLE,
# MAGIC     ghs_avg_order DECIMAL(37,23),
# MAGIC     george_avg_order DECIMAL(38,21),
# MAGIC     ghs_orders_orderers DOUBLE,
# MAGIC     george_orders_orderers DOUBLE,
# MAGIC     ghs_sales_mailed DECIMAL(37,23),
# MAGIC     george_sales_mailed DECIMAL(38,21),
# MAGIC     ghs_items_order DOUBLE,
# MAGIC     george_items_order DOUBLE
# MAGIC ) 
# MAGIC
# MAGIC      COMMENT "This table contains something"
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
