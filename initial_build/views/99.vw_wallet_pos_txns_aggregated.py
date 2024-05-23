# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create - vw_wallet_pos_txns_aggregated

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_wallet_pos_txns_aggregated";

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

# MAGIC %md
# MAGIC ###Create view query
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC SELECT main3.*,
# MAGIC   activecount.active_cum,
# MAGIC   null as acct_status_id --null is needed as removing this column causes issues in Power BI, which expects it
# MAGIC FROM
# MAGIC   -- Show the date, store number, number of distinct scanners for that day / store, and number of users per day in total
# MAGIC   (
# MAGIC     select
# MAGIC       main.*,
# MAGIC       main2.num_users_scanning
# MAGIC     from
# MAGIC       -- Calculate number of unique users per day / store
# MAGIC       (
# MAGIC         select
# MAGIC           cast(event_ts as date) as event_dt,
# MAGIC           store_nbr,
# MAGIC           count(wallet_id) as num_transactions
# MAGIC         from
# MAGIC           ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns
# MAGIC         group by
# MAGIC           cast(event_ts as date),
# MAGIC           store_nbr
# MAGIC         order by
# MAGIC           event_dt
# MAGIC       ) main
# MAGIC       left join -- Bring in number of distinct users per day
# MAGIC       (
# MAGIC         select
# MAGIC           cast(event_ts as date) as event_dt,
# MAGIC           count (distinct wallet_id) as num_users_scanning
# MAGIC         from
# MAGIC           ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns w
# MAGIC         group by
# MAGIC           cast(event_ts as date)
# MAGIC       ) main2 on main.event_dt = main2.event_dt
# MAGIC   ) main3
# MAGIC   left join (
# MAGIC     -- Calculate date and cumulative users up to and including that date
# MAGIC     /*
# MAGIC         The below code calculates the cumulative number of users by finding each distinct wallet id, and its first (min) scan date.
# MAGIC         This is then overwritten as '1'. 
# MAGIC         Finally SUM() OVER() then sums these by each day.
# MAGIC         */
# MAGIC     SELECT
# MAGIC       DISTINCT dt as dt,
# MAGIC       sum (cnt) OVER (
# MAGIC         ORDER BY
# MAGIC           dt asc
# MAGIC       ) as active_cum
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           dt,
# MAGIC           1 as cnt
# MAGIC         FROM
# MAGIC           (
# MAGIC             SELECT
# MAGIC               DISTINCT wallet_id,
# MAGIC               min(cast(event_ts as date)) dt
# MAGIC             FROM
# MAGIC               ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns
# MAGIC             GROUP BY
# MAGIC               wallet_id
# MAGIC           )
# MAGIC         ORDER BY
# MAGIC           dt desc
# MAGIC       )
# MAGIC     ORDER BY
# MAGIC       dt desc
# MAGIC   ) activecount on main3.event_dt = activecount.dt
# MAGIC ORDER BY
# MAGIC   event_dt desc;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
