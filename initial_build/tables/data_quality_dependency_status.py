# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_dependency_status";

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
# MAGIC      check_id INT,
# MAGIC      check_definition STRING COMMENT "Definition/description of the data quality check.",
# MAGIC      severity STRING NOT NULL COMMENT "If set to High then the overall status of the depdency won't fail",
# MAGIC      dts timestamp COMMENT "Time when record was captured",
# MAGIC      status boolean COMMENT "Pass/Fail",
# MAGIC      overall_status boolean COMMENT "overall status is false if any of the dependant has is failed"
# MAGIC      ) 
# MAGIC      COMMENT "The data_quality_dependency_status table tracks the status of data quality checks and their dependencies. It includes information on the definition/description of the check, the type of dependency (i.e. report or data bricks workflow), and the name of the object dependent on this check (i.e. report name or workflow name). The table also captures the time when the record was captured and whether the check passed or failed. The overall_status column indicates the overall status of all checks and their dependencies."
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC
# MAGIC
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
