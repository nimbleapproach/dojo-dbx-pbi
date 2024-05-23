# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_check_historical_snapshot";

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
# MAGIC CREATE TABLE IF NOT EXISTS  ${widget.catalog}.${widget.schema}.${widget.table_name} (
# MAGIC      check_id INT, --Match ID in check_definition and check_status
# MAGIC      history_type STRING COMMENT "Type of history we are recording" ,
# MAGIC      historical_value STRING COMMENT "The historical value recorded",
# MAGIC      dts TIMESTAMP DEFAULT current_timestamp() COMMENT "Time when record was captured",
# MAGIC      CONSTRAINT pk_${widget.table_name} PRIMARY KEY(check_id, history_type, dts)
# MAGIC      ) 
# MAGIC      COMMENT "Table to record and update historical expections of source data."
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2',
# MAGIC          'delta.feature.allowColumnDefaults' = 'supported' 
# MAGIC          );
# MAGIC
# MAGIC         
# MAGIC     
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
