# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_dependency_run_log";

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
# MAGIC      dependency_id INT NOT NULL,
# MAGIC      dts TIMESTAMP NOT NULL ,
# MAGIC      type STRING NOT NULL COMMENT "The type of dependency",
# MAGIC      name STRING NOT NULL COMMENT "Dependency name",
# MAGIC      workflow_id STRING COMMENT "ID specific to the workflow run",
# MAGIC      status STRING COMMENT "Status specific to dependency type",
# MAGIC      schedule STRING COMMENT "The intended run schedule.",
# MAGIC      run_status_flag BOOLEAN COMMENT "The run status based on schedule.",
# MAGIC      start_time STRING COMMENT "The workflow run start time",
# MAGIC      end_time STRING COMMENT "The end time if the run has ended",
# MAGIC      duration_in_seconds STRING COMMENT "THe run duration in seconds if it has finished.",
# MAGIC      CONSTRAINT pk_${widget.table_name} PRIMARY KEY(dependency_id, status, dts)
# MAGIC      ) 
# MAGIC      COMMENT "This table contains information from the table data_quality_dependency_info, formatted for reporting."
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Add error message col

# COMMAND ----------

run_url_column = spark.sql(f"""
          SELECT * FROM {catalog}.information_schema.columns
            WHERE table_catalog ='{catalog}'
            AND table_schema = '{schema}'
            AND table_name = '{table_name}'
            AND column_name = 'error_message'
          """)

if run_url_column.count() == 0:
    spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table_name} ADD COLUMN error_message STRING COMMENT 'Any error message in recent job run'
              """)
