# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "loyalty_acct_archive";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets INTo local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values INTo the local variable and removing widgets. 
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
spark.conf.set ('widget.table_name', dbutils.widgets.get("table_name"))

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table_name = 'loyalty_acct_archive'

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
# MAGIC  wallet_id STRING,
# MAGIC  scheme_id INT,
# MAGIC  singl_profl_id STRING,
# MAGIC  bluelight_id STRING,
# MAGIC  curr_cash_bnk_pnts_qty DOUBLE,
# MAGIC  lifetime_pnts_qty DOUBLE,
# MAGIC  last_shopped_store_id STRING,
# MAGIC  src_modfd_ts TIMESTAMP,
# MAGIC  last_loyalty_scan_ts TIMESTAMP,
# MAGIC  acct_status_id STRING,
# MAGIC  event_ts TIMESTAMP,
# MAGIC  bluelight_id_last_upd_ts TIMESTAMP,
# MAGIC  payload_type STRING,
# MAGIC  event_dt DATE
# MAGIC     ) 
# MAGIC      COMMENT "This table contains something"
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
# MAGIC

# COMMAND ----------

laa = spark.sql(f""" SELECT * FROM {catalog}.{schema}.{table_name}""")
        
cols_to_drop = ['scheme_id',
        'singl_profl_id',
        'bluelight_id',
        'curr_cash_bnk_pnts_qty',
        'lifetime_pnts_qty',
        'last_shopped_store_id',
        'src_modfd_ts',
        'last_loyalty_scan_ts',
        'acct_status_id',
        'event_ts',
        'payload_type',
        'event_dt']
laa = laa.drop(*cols_to_drop)
laa.createOrReplaceTempView('laa_load')

display(spark.sql(f"""DROP TABLE {catalog}.{schema}.{table_name}"""))
display(spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} AS SELECT * FROM laa_load"""))


