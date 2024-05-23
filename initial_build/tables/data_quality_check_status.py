# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "data_quality_check_status";

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
# MAGIC      check_id INT,
# MAGIC      definition STRING COMMENT "Definition/description of the data quality check." ,
# MAGIC      status BOOLEAN COMMENT "Pass/Fail",
# MAGIC      dts TIMESTAMP DEFAULT current_timestamp() COMMENT "Time when record was captured",
# MAGIC      error_message STRING COMMENT "Log error message if the relevant notebook for this check has failed.",
# MAGIC      CONSTRAINT pk_${widget.table_name} PRIMARY KEY(check_id,definition,status,dts)
# MAGIC      ) 
# MAGIC      COMMENT "Table to keep data quality checks definition"
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2',
# MAGIC          'delta.feature.allowColumnDefaults' = 'supported' 
# MAGIC          );
# MAGIC        

# COMMAND ----------

# MAGIC %md
# MAGIC Add columns to capture run_url and run duration for debugging

# COMMAND ----------

run_url_column = spark.sql(f"""
          SELECT * FROM {catalog}.information_schema.columns
            WHERE table_catalog ='{catalog}'
            AND table_schema = '{schema}'
            AND table_name = '{table_name}'
            AND column_name = 'run_url'
          """)

if run_url_column.count() == 0:
    spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table_name} ADD COLUMN run_url STRING COMMENT 'URL of job run where it has been recorded'
              """)
    spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table_name} ALTER COLUMN run_url SET DEFAULT null
              """)

# COMMAND ----------

run_duration_column = spark.sql(f"""
          SELECT * FROM {catalog}.information_schema.columns
            WHERE table_catalog ='{catalog}'
            AND table_schema = '{schema}'
            AND table_name = '{table_name}'
            AND column_name = 'run_duration_seconds'
          """)

if run_duration_column.count() == 0:
    spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table_name} ADD COLUMN run_duration_seconds FLOAT COMMENT 'The run duration in seconds.'
              """)
    spark.sql(f"""
                ALTER TABLE {catalog}.{schema}.{table_name} ALTER COLUMN run_duration_seconds SET DEFAULT null
              """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
