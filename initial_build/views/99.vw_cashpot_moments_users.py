# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_cashpot_moments_users";

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
# MAGIC with transfers as (
# MAGIC
# MAGIC SELECT ca.wallet_id_dest
# MAGIC     , ca.trans_type
# MAGIC     , rsn_cd
# MAGIC     , cba.scheme_id as src_scheme_id
# MAGIC     , casa.cashpot_nm as src_cashpot_nm
# MAGIC     , cbb.scheme_id as dest_scheme_id
# MAGIC     , casb.cashpot_nm as dest_cashpot_nm
# MAGIC     , cashpot_val_dest as val_trans
# MAGIC     , cast(ca.trans_ts as date) as trans_dt
# MAGIC     , row_number() OVER (PARTITION BY ca.wallet_id_dest ORDER BY ca.trans_ts asc) as row_num
# MAGIC  
# MAGIC FROM ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_cashpot_adjs ca
# MAGIC JOIN  ${widget.core_catalog}.gb_mb_dl_tables.cashpot_bal cba
# MAGIC ON ca.cashpot_id_src = cba.cashpot_id
# MAGIC JOIN ${widget.core_catalog}.gb_mb_dl_tables.cashpot_setup casa
# MAGIC ON cba.scheme_id = casa.scheme_id and casa.rec_status_ind LIKE 'CURRENT'
# MAGIC JOIN ${widget.core_catalog}.gb_mb_dl_tables.cashpot_bal cbb
# MAGIC ON ca.cashpot_id_dest = cbb.cashpot_id
# MAGIC JOIN ${widget.core_catalog}.gb_mb_dl_tables.cashpot_setup casb
# MAGIC ON cbb.scheme_id = casb.scheme_id and casb.rec_status_ind LIKE 'CURRENT'
# MAGIC
# MAGIC WHERE ca.rsn_cd LIKE 'TRANSFER'
# MAGIC )
# MAGIC
# MAGIC SELECT trans_dt, src_scheme_id, src_cashpot_nm, dest_scheme_id, dest_cashpot_nm, count(distinct wallet_id_dest) as num_first_users
# MAGIC FROM transfers
# MAGIC WHERE trans_dt < current_date() and row_num = 1
# MAGIC GROUP BY trans_dt, src_scheme_id, src_cashpot_nm, dest_scheme_id, dest_cashpot_nm
# MAGIC order by trans_dt
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
