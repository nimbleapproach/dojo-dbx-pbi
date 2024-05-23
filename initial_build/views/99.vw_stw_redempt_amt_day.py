# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_stw_redmpt_amt_day";

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
# MAGIC SELECT
# MAGIC   *,
# MAGIC   (game_win_pounds * customer_count) as giveaway
# MAGIC FROM
# MAGIC   (
# MAGIC     select
# MAGIC       cast(game_redm_ts as date) as game_redm_dt,
# MAGIC       round(cast(game_win_val as float) * 0.01, 2) as game_win_pounds,
# MAGIC       count(*) as customer_count
# MAGIC     from
# MAGIC       ${widget.core_catalog}.gb_mb_dl_tables.game_wallets
# MAGIC     where
# MAGIC       rec_status_ind LIKE 'CURRENT'
# MAGIC       and game_redm_ts is not null
# MAGIC       and cast(game_redm_ts as date) < current_date()
# MAGIC     GROUP BY
# MAGIC       cast(game_redm_ts as date),
# MAGIC       game_win_val
# MAGIC   )
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
