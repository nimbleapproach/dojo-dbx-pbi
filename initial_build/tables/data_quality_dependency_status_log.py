# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_dependency_status_log";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
# MAGIC
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
# MAGIC CREATE TABLE IF NOT EXISTS ${widget.catalog}.${widget.schema}.${widget.table_name} (
# MAGIC      dependency_id INT,
# MAGIC      dependency_type STRING COMMENT "Type of dependency i.e. report or data bricks workflow", 
# MAGIC      dependency_name STRING COMMENT "Name of the object dependent on this check i.e. report name or workflow name",
# MAGIC      overall_status BOOLEAN COMMENT "The dependency overall status",
# MAGIC      dts TIMESTAMP DEFAULT current_timestamp() COMMENT "Time when record was captured"
# MAGIC      ) 
# MAGIC      COMMENT "Log of how dependency availability changes over time."
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2',
# MAGIC          'delta.feature.allowColumnDefaults' = 'supported' 
# MAGIC          );
# MAGIC
# MAGIC
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
