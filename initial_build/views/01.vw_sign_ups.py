# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_sign_ups";
# MAGIC

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
# MAGIC         lyl.singl_profl_id
# MAGIC         ,cast(lyl.regtn_ts as date) marvel_signup_dt
# MAGIC         ,case when (cust.registration_channel = 'LOYALTY' or cast(cust.registration_date as date) = cast(cust.tnc_accepted_at_loyalty as date) )THEN 1 else 0 END AS new_ind
# MAGIC         ,case when rpt_lyl.mktg_consent_email ='Y' OR rpt_lyl.push_notif_consent  ='Y' then 1 else 0 end as opt_in_flag
# MAGIC         ,case when (regtn_ts< (cast(current_timestamp as timestamp) - INTERVAL 888 HOURS)) and lyl.acct_status_id='TEMP' then 1 else 0
# MAGIC         end as acct_del_ind_1
# MAGIC         ,lyl.acct_status_id
# MAGIC         from
# MAGIC     ( 
# MAGIC         select DISTINCT case when singl_profl_id is not null then singl_profl_id
# MAGIC             when singl_profl_id is null then concat('no_assgnd_spid_', cast   (wallet_id as varchar(50)))
# MAGIC             end as singl_profl_id
# MAGIC             ,regtn_ts
# MAGIC             ,acct_status_id
# MAGIC                         ,case when regtn_ts <> dt_cnvrt_to_full_acct_ts then 1
# MAGIC         when dt_cnvrt_to_full_acct_ts is null  then 1 else 0 
# MAGIC     end as guest_ind
# MAGIC                 from ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct
# MAGIC         where 1=1
# MAGIC         and cast(regtn_ts as date) < CURRENT_DATE
# MAGIC     ) lyl
# MAGIC     LEFT OUTER JOIN ${widget.core_catalog}.gb_customer_data_domain_secured_rpt.cdd_rpt_loyalty_acct rpt_lyl
# MAGIC         ON lyl.singl_profl_id = rpt_lyl.singl_profl_id
# MAGIC     LEFT OUTER JOIN ${widget.core_catalog}.gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer cust
# MAGIC         ON lyl.singl_profl_id = cust.singl_profl_id ;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
