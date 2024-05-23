# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to fix data in tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
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

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Following command is removing duplicates

# COMMAND ----------

# Load the table
df = spark.table("${widget.catalog}.${widget.schema}.active_earners")
 
# Drop duplicates based on the full_date column
df = df.dropDuplicates(["full_date"])
 
# Overwrite the original table with the resulting dataframe
df.write.mode("overwrite").saveAsTable("${widget.catalog}.${widget.schema}.active_earners")
