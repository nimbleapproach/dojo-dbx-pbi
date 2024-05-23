# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_active_users_breakdown_tbl";

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
# MAGIC   case
# MAGIC     when a.week_cat = '1' then 1
# MAGIC     when a.week_cat = '2' then 2
# MAGIC     when a.week_cat = '3' then 3
# MAGIC     when a.week_cat = '4' then 4
# MAGIC     when a.week_cat = '5' then 5
# MAGIC     when a.week_cat = '6' then 6
# MAGIC     when a.week_cat = '7' then 7
# MAGIC     when a.week_cat = '8' then 8
# MAGIC     when a.week_cat = '9' then 9
# MAGIC     when a.week_cat = '10' then 10
# MAGIC     when a.week_cat = '11' then 11
# MAGIC     when a.week_cat = '12' then 12
# MAGIC     when a.week_cat = '13' then 13
# MAGIC     when a.week_cat = 'Lapsed' then 14
# MAGIC     else null
# MAGIC   end as index,
# MAGIC   case
# MAGIC     when a.week_cat = '1' then '0 - 1 Weeks Ago'
# MAGIC     when a.week_cat = '2' then '1 - 2 Weeks Ago'
# MAGIC     when a.week_cat = '3' then '2 - 3 Weeks Ago'
# MAGIC     when a.week_cat = '4' then '3 - 4 Weeks Ago'
# MAGIC     when a.week_cat = '5' then '4 - 5 Weeks Ago'
# MAGIC     when a.week_cat = '6' then '5 - 6 Weeks Ago'
# MAGIC     when a.week_cat = '7' then '6 - 7 Weeks Ago'
# MAGIC     when a.week_cat = '8' then '7 - 8 Weeks Ago'
# MAGIC     when a.week_cat = '9' then '8 - 9 Weeks Ago'
# MAGIC     when a.week_cat = '10' then '9 - 10 Weeks Ago'
# MAGIC     when a.week_cat = '11' then '10 - 11 Weeks Ago'
# MAGIC     when a.week_cat = '12' then '11 - 12 Weeks Ago'
# MAGIC     when a.week_cat = '13' then '12 - 13 Weeks Ago'
# MAGIC     when a.week_cat = 'Lapsed' then 'Lapsed'
# MAGIC     else null
# MAGIC   end as heading,
# MAGIC   a.*,
# MAGIC   a.total - b.total as total_wk_chng,
# MAGIC   a.new_one_scanners - b.new_one_scanners as first_week_wk_chng,
# MAGIC   a.new_active - b.new_active as new_wk_chng,
# MAGIC   a.recycling_active - b.recycling_active as retained_wk_chng,
# MAGIC   a.reactive_active - b.reactive_active as reactive_wk_chng
# MAGIC from
# MAGIC   ${widget.catalog}.${widget.schema}.vw_active_users_funnel a
# MAGIC   join ${widget.catalog}.${widget.schema}.vw_active_users_funnel_last_week b on a.week_cat = b.week_cat
# MAGIC order by
# MAGIC   index asc
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
