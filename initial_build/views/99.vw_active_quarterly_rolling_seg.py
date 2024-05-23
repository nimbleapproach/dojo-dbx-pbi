# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_active_quarterly_rolling_seg";

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
# MAGIC with mnth_dates as (
# MAGIC   select
# MAGIC     cal_month as cal_month_tag,
# MAGIC     month_end as cal_month,
# MAGIC     month_end as month_end,
# MAGIC     date_add(month_end, -90) as rol_qrt_start --90 not 91 because its inclusive
# MAGIC   FROM
# MAGIC     (
# MAGIC       select
# MAGIC         max(day_date) as month_end,
# MAGIC         cal_month
# MAGIC       FROM(
# MAGIC           select
# MAGIC             day_date,
# MAGIC             SUBSTRING(day_date, 0, 7) as cal_month
# MAGIC           from
# MAGIC             ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
# MAGIC           WHERE
# MAGIC             day_date >= cast('2022-08-01' as date)
# MAGIC         )
# MAGIC       GROUP BY
# MAGIC         cal_month
# MAGIC     )
# MAGIC   WHERE
# MAGIC     month_end <= current_date()
# MAGIC )
# MAGIC select
# MAGIC   md.month_end,
# MAGIC   COUNT(distinct wallet_id) active_quarter
# MAGIC FROM
# MAGIC   mnth_dates md
# MAGIC   JOIN ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns pos ON cast(event_ts as date) between md.rol_qrt_start
# MAGIC   and md.month_end
# MAGIC GROUP BY
# MAGIC   md.month_end
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
