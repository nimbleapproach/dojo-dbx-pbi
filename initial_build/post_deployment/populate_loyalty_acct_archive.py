# Databricks notebook source
# MAGIC %md
# MAGIC ###Creating widgets
# MAGIC ######Input widgets and apply parameters 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC

# COMMAND ----------


spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))

catalog = dbutils.widgets.get("catalog")
schema =  dbutils.widgets.get("schema")


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.core_catalog}' AS core_catalog,
# MAGIC        '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Populate Loyalty acct archive

# COMMAND ----------

#Method using volume:

file_name = 'loyalty_acct_archive_load.parquet'
laa = spark.read.parquet(f'/Volumes/{catalog}/{schema}/ext_vol_{schema}/loyalty_account_archive/{file_name}')
laa.createOrReplaceTempView('laa_load')
display(spark.sql(f"""
INSERT OVERWRITE TABLE {catalog}.{schema}.loyalty_acct_archive
    SELECT * FROM laa_load
"""))


