# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_dependency_info";

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
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${widget.catalog}.${widget.schema}.${widget.table_name} (
# MAGIC      dependency_id INT NOT NULL,
# MAGIC      info STRING NOT NULL COMMENT "Json to keep dependency information from API request",
# MAGIC      dts TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT "Time when record was captured"
# MAGIC      ) 
# MAGIC      COMMENT "The data_quality_dependency_info table records information about each dependency from API requests."
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2',
# MAGIC          'delta.feature.allowColumnDefaults' = 'supported' 
# MAGIC          );
# MAGIC
# MAGIC ALTER TABLE ${widget.catalog}.${widget.schema}.${widget.table_name} ALTER COLUMN info SET NOT NULL;
# MAGIC ALTER TABLE ${widget.catalog}.${widget.schema}.${widget.table_name} DROP PRIMARY KEY IF EXISTS CASCADE;
# MAGIC ALTER TABLE ${widget.catalog}.${widget.schema}.${widget.table_name} ADD CONSTRAINT pk_${widget.table_name} PRIMARY KEY(dependency_id,info);
# MAGIC
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
