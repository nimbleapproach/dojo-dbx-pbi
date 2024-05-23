# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_liability_reconciliation";

# COMMAND ---------

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
# MAGIC with cte as 
# MAGIC (
# MAGIC         SELECT index, type, sum(val) as val, dt
# MAGIC         FROM(
# MAGIC         select case when cmpgn_id = 508017 then 2 else 1 end as index
# MAGIC             , case when cmpgn_id = 508017 then 'total cashpot from blue light (historic)' 
# MAGIC                 else 'total cashpot from campaigns (excl. Blue Light) (historic)' end as type
# MAGIC             , sum(mission_value_pounds_new) val
# MAGIC             , left(cast(cmpgn_cmplt_ts as date),7) dt
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_mssn_wallets mw
# MAGIC         GROUP BY left(cast(cmpgn_cmplt_ts as date),7), cmpgn_id --did it this way because the query was too big otherwise
# MAGIC         )
# MAGIC         GROUP BY index, type, dt
# MAGIC
# MAGIC
# MAGIC         UNION
# MAGIC         select 3 index, 'goodwill (historic)' type,  round(sum(cast(cr_pnts_qty as float)/100), 2) val, left(cast(event_ts as date),7) dt
# MAGIC         from ${widget.core_catalog}.gb_mb_dl_tables.cs_credits
# MAGIC         where event_nm = 'GOODWILL'
# MAGIC         GROUP BY left(cast(event_ts as date),7)
# MAGIC
# MAGIC         UNION
# MAGIC         select 4 index, 'credit cards' type, round(sum(cast(trans_amt as float)/100), 2) val, left(cast(event_ts as date),7) dt
# MAGIC         from ${widget.core_catalog}.gb_mb_dl_tables.ext_trans
# MAGIC         where chnl_nm like 'Ja%'
# MAGIC         GROUP BY left(cast(event_ts as date),7)
# MAGIC
# MAGIC         UNION
# MAGIC         select 5 index, 'colleague incentive (historic)' type,  round(sum(cast(trans_amt as float)/100), 2) val, left(cast(event_ts as date),7) dt
# MAGIC         from ${widget.core_catalog}.gb_mb_dl_tables.ext_trans
# MAGIC         where chnl_nm not like 'Ja%'
# MAGIC         GROUP BY left(cast(event_ts as date),7)
# MAGIC
# MAGIC         UNION
# MAGIC         (SELECT case when acct_status_id LIKE 'DEFAULT' THEN 6
# MAGIC                 ELSE 7 END AS index
# MAGIC             , case when acct_status_id LIKE 'DEFAULT' THEN 'Expired from cashpots (Default)'
# MAGIC                 ELSE 'Expired from cashpots (TEMP)' END AS type
# MAGIC             , round(coalesce(sum(cast(cashpot_val_exp as float)/100),0.00),2) val
# MAGIC             , left(event_ts, 7) dt
# MAGIC         FROM
# MAGIC         (
# MAGIC         SELECT o.*
# MAGIC             , case when l.acct_status_id is null then 'TEMP'
# MAGIC                 else cast(l.acct_status_id as string) 
# MAGIC                 end as acct_status_id
# MAGIC         FROM  ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_cashpot_movemnt o
# MAGIC         LEFT JOIN ${widget.catalog}.${widget.schema}.vw_loyalty_acct l
# MAGIC             on o.wallet_id = l.wallet_id
# MAGIC         )
# MAGIC         GROUP BY left(event_ts, 7), acct_status_id)
# MAGIC
# MAGIC         UNION
# MAGIC         select 8 index, 'vouchers created (historic)' type, sum(voucher_value_pounds) val, left(cast(reward_gained_ts as date),7) dt
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_reward_wallets
# MAGIC         GROUP BY left(cast(reward_gained_ts as date),7)
# MAGIC
# MAGIC         UNION
# MAGIC         select 9 index, 'vouchers expired (historic)' type, -sum(voucher_value_pounds_expired) val, left(cast(reward_end_dt as date),7) dt
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_expired_vouchers
# MAGIC         GROUP BY left(cast(reward_end_dt as date),7)
# MAGIC
# MAGIC         UNION
# MAGIC         select 10 index, 'vouchers redeemed (historic)' type, -sum(voucher_value_pounds_redeemed) val, left(cast(reward_redm_dt as date),7) dt
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_reward_wallets_aggregated
# MAGIC         where left(cast(reward_redm_dt as date),7) is not null
# MAGIC         GROUP BY left(cast(reward_redm_dt as date),7)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC SELECT * FROM cte
# MAGIC
# MAGIC UNION
# MAGIC -- calculate the remaining liability (all earns less expired cashpot less redeemed and expired vouchers)
# MAGIC SELECT 11 index, 'Liability remaining' type, sum(val) OVER (PARTITION BY dt) val, dt
# MAGIC FROM cte
# MAGIC WHERE index  between 1 and 7 or index = 9 or index = 10
# MAGIC order by index asc, dt asc;
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10; 
# MAGIC
# MAGIC
