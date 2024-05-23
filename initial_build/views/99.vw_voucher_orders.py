# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create - vw_voucher_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_voucher_orders";

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
# MAGIC SELECT singl_profl_id,
# MAGIC     promo_id,
# MAGIC     a.web_order_id,
# MAGIC     disc_coupn_cd,
# MAGIC     order_status_nm,
# MAGIC     evouch_tot_amt,
# MAGIC     dlvr_dt,
# MAGIC     b.src_create_ts,
# MAGIC     promo_nm,
# MAGIC     pos_tot_amt
# MAGIC FROM  ${widget.core_catalog}.gb_mb_dl_tables.ghs_order_promo a
# MAGIC INNER JOIN ${widget.core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka b
# MAGIC     ON a.web_order_id=b.web_order_id
# MAGIC WHERE disc_coupn_cd is not null
# MAGIC AND seq_nbr = 1  -- Added in by MN as some orders were being doubled in the join due to multiple records
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
