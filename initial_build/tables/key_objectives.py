# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to create table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "key_objectives";

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
# MAGIC       date DATE COMMENT "Date query ran to append row.",
# MAGIC       app_rating FLOAT COMMENT "Average rating of apple/play store ratings",
# MAGIC       active_members INT COMMENT "Distinct wallets from wallet_pos in l13w",
# MAGIC       active_connected_cust INT COMMENT "Active connected customers (Direct)",
# MAGIC       invested_into_cashpot INT COMMENT "sum of earn_value_gbp - invested from campaigns"
# MAGIC     ) 
# MAGIC      COMMENT "This table contains daily ls eleven key objective results"
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
