# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_check_status_timeseries";

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

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table_name")

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
# MAGIC      check_id INT NOT NULL,
# MAGIC      hour_of_day INTEGER NOT NULL COMMENT "The hour of the day, from 0 to 23." ,
# MAGIC      pass_rate_last_7_days FLOAT NOT NULL COMMENT "The pass rate of the check, at this hour, over the past week.",
# MAGIC      pass_rate_last_30_days FLOAT NOT NULL COMMENT "The pass rate of the check, at this hour, over the past month.",
# MAGIC      CONSTRAINT pk_${widget.table_name} PRIMARY KEY(check_id,hour_of_day)
# MAGIC      ) 
# MAGIC      COMMENT "Table contains information on how the pass rate of checks changes throughout the day."
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2',
# MAGIC          'delta.feature.allowColumnDefaults' = 'supported' 
# MAGIC          );
# MAGIC        

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
