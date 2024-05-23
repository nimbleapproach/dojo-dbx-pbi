# Databricks notebook source
# MAGIC %md
# MAGIC # Clear history data for checks 1 and 2 (mssn_wallets checks)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS database



# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.data_quality_check_historical_snapshot
# MAGIC WHERE check_id=1 or check_id = 2       
# MAGIC 
# MAGIC

