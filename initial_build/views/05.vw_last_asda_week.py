# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ---------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_last_asda_week";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
spark.conf.set ('widget.view_name', dbutils.widgets.get("view_name"))


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.text('view_name', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.core_catalog}' AS core_catalog,
# MAGIC        '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS schema,
# MAGIC        '${widget.view_name}' AS view_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC select 
# MAGIC     min(full_date) as start_date
# MAGIC     ,max(full_date) as end_date
# MAGIC from ${widget.catalog}.${widget.schema}.vw_date_table
# MAGIC where asda_week = 
# MAGIC     -- Select the previous asda week
# MAGIC     (select * 
# MAGIC     from (
# MAGIC         -- Select the current and previous asda week
# MAGIC         select distinct(asda_week)
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_date_table --this table contains every day up until yesterday, along with its week
# MAGIC         order by asda_week desc
# MAGIC         limit 2) 
# MAGIC     order by asda_week asc --note how this orders the weeks in ascending order, i.e. select the last week
# MAGIC     limit 1);
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
