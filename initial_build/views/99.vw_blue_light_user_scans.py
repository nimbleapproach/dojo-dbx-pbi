# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_blue_light_user_scans";

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
# MAGIC WITH blue_lyl_acct AS
# MAGIC (SELECT la.wallet_id
# MAGIC     , la.singl_profl_id
# MAGIC     , la.bluelight_id
# MAGIC     , coalesce(cast(laa.bluelight_id_last_upd_ts as DATE), cast(la.bluelight_id_last_upd_ts as DATE)) as blue_light_upd_ts
# MAGIC FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC LEFT OUTER JOIN(
# MAGIC    select wallet_id
# MAGIC        , min(bluelight_id_last_upd_ts) as bluelight_id_last_upd_ts
# MAGIC    from ${widget.catalog}.${widget.schema}.loyalty_acct_archive
# MAGIC    WHERE bluelight_id_last_upd_ts is not null
# MAGIC    GROUP BY wallet_id
# MAGIC    )laa
# MAGIC ON la.wallet_id = laa.wallet_id
# MAGIC WHERE la.bluelight_id_last_upd_ts IS NOT NULL)
# MAGIC
# MAGIC
# MAGIC SELECT cast(wpt.event_ts as date) as scan_dt
# MAGIC     , wpt.store_nbr
# MAGIC     , count(*) as Blue_Light_Scans
# MAGIC     
# MAGIC FROM blue_lyl_acct bla
# MAGIC JOIN ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC
# MAGIC ON bla.wallet_id = wpt.wallet_id
# MAGIC     and bla.blue_light_upd_ts <= wpt.event_ts
# MAGIC WHERE wpt.event_ts < current_date()
# MAGIC
# MAGIC GROUP BY cast(wpt.event_ts as date), wpt.store_nbr
# MAGIC order by store_nbr, scan_dt
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
# MAGIC
