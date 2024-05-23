# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_coupon_redm";

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
# MAGIC   cast(coupn_redm_ts as date) as redm_dt,
# MAGIC   cmpgn_id,
# MAGIC   case
# MAGIC     when wpt.store_nbr = '-1' then ti.store_nbr
# MAGIC     else wpt.store_nbr
# MAGIC   end as store_nbr,
# MAGIC   chnl_nm,
# MAGIC   sum(rdmpt_cnt) as redm_cnt,
# MAGIC   sum(rdmpt_val * 0.01) as redm_pounds,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when wpt.chnl_nm like 'ecom' then rdmpt_cnt
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as online_redm,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when wpt.chnl_nm like 'store' then rdmpt_cnt
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as store_redm,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when wpt.chnl_nm like 'ecom' then rdmpt_val * 0.01
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as online_pounds,
# MAGIC   sum(
# MAGIC     case
# MAGIC       when wpt.chnl_nm like 'store' then rdmpt_val * 0.01
# MAGIC       else 0
# MAGIC     end
# MAGIC   ) as store_pounds
# MAGIC from
# MAGIC   ${widget.core_catalog}.gb_mb_dl_tables.coupn_wallets cw
# MAGIC   left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt 
# MAGIC   on cw.wallet_id = wpt.wallet_id
# MAGIC   and cw.coupn_redm_ts = wpt.event_ts
# MAGIC   left join ${widget.core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka ti 
# MAGIC   ON wpt.trans_rcpt_nbr = ti.web_order_id
# MAGIC where
# MAGIC   cw.coupn_redm_ts is not null
# MAGIC   and cw.rec_status_ind like 'CURRENT'
# MAGIC   and cw.coupn_redm_ts < current_date()
# MAGIC group by 1, 2, 3, 4
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
