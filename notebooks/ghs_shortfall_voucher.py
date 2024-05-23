# Databricks notebook source
# DBTITLE 0,Notebook to populate ghs_shortfall_voucher table
# MAGIC %md
# MAGIC ##Process:     ghs_shortfall_voucher
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating widgets
# MAGIC ######Input widgets and apply parameters 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";

# COMMAND ----------

spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.core_catalog}' AS core_catalog,
# MAGIC        '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation
# MAGIC ######Query coreprod data into a temporay view  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vwGHS_ShortFall_Voucher
# MAGIC AS
# MAGIC
# MAGIC with 
# MAGIC wallet_pos as
# MAGIC (
# MAGIC SELECT trans_rcpt_nbr, wallet_id, event_ts
# MAGIC FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns
# MAGIC where chnl_nm = 'ecom'
# MAGIC ),
# MAGIC groc_refund as
# MAGIC (
# MAGIC SELECT order_id, singl_profl_userid, order_dlvr_dt,TO_DATE(upd_ts) as rfnd_dt, sum(rfnd_amt) as total_rfnd_amt
# MAGIC FROM ${widget.core_catalog}.gb_mb_dl_tables.groc_order_rfnd
# MAGIC where (rfnd_status_cd IS NULL OR rfnd_status_cd = 'COMPLETE')
# MAGIC group by order_id, singl_profl_userid, order_dlvr_dt, TO_DATE(upd_ts)
# MAGIC ),
# MAGIC groc_kafka as
# MAGIC (
# MAGIC SELECT web_order_id, dlvr_dt,pos_tot_amt,order_sub_tot_amt, web_order_tot_amt, singl_profl_id
# MAGIC FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka
# MAGIC where order_status_nm != 'CANCELLED' and dlvr_dt > '2022-11-14'
# MAGIC order by dlvr_dt
# MAGIC ),
# MAGIC reward_wallets as
# MAGIC (
# MAGIC SELECT wallet_id, cmpgn_id, reward_redm_ts
# MAGIC FROM ${widget.core_catalog}.gb_mb_dl_tables.reward_wallets
# MAGIC where reward_status_desc = 'USED' and rec_status_ind = 'CURRENT'
# MAGIC ),
# MAGIC cmpgn_setup as
# MAGIC (
# MAGIC SELECT cmpgn_id, disc_amt/100.0 as rewards_voucher_val 
# MAGIC FROM ${widget.core_catalog}.gb_mb_dl_tables.cmpgn_setup
# MAGIC where cmpgn_clsfctn_nm = 'COUPON' and cmpgn_status_desc = 'ACTIVE' and rec_status_ind = 'CURRENT'
# MAGIC ),
# MAGIC loyalty_acct as
# MAGIC (
# MAGIC SELECT wallet_id, loyalty_id
# MAGIC FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct
# MAGIC )
# MAGIC
# MAGIC SELECT wallet_pos.wallet_id, groc_kafka.singl_profl_id, loyalty_acct.loyalty_id,
# MAGIC groc_kafka.web_order_id,
# MAGIC TO_DATE(wallet_pos.event_ts) as dlvr_dt,
# MAGIC groc_kafka.web_order_tot_amt, groc_refund.total_rfnd_amt,
# MAGIC groc_refund.rfnd_dt,
# MAGIC sum(cmpgn_setup.rewards_voucher_val) as total_rewards_voucher_val,
# MAGIC CASE
# MAGIC WHEN groc_refund.total_rfnd_amt > 0 THEN 1
# MAGIC ELSE 0
# MAGIC END AS rfnd_ind
# MAGIC from wallet_pos
# MAGIC inner join groc_kafka
# MAGIC on wallet_pos.trans_rcpt_nbr = groc_kafka.web_order_id
# MAGIC inner join groc_refund
# MAGIC on groc_kafka.web_order_id = groc_refund.order_id
# MAGIC left join reward_wallets
# MAGIC on wallet_pos.wallet_id = reward_wallets.wallet_id 
# MAGIC and wallet_pos.event_ts = reward_wallets.reward_redm_ts
# MAGIC left join cmpgn_setup
# MAGIC on reward_wallets.cmpgn_id = cmpgn_setup.cmpgn_id
# MAGIC LEFT JOIN loyalty_acct
# MAGIC ON wallet_pos.wallet_id = loyalty_acct.wallet_id
# MAGIC where rewards_voucher_val > 0
# MAGIC group by wallet_pos.wallet_id, groc_kafka.singl_profl_id,loyalty_acct.loyalty_id, groc_kafka.web_order_id,
# MAGIC wallet_pos.event_ts, groc_kafka.web_order_tot_amt, groc_refund.total_rfnd_amt, 
# MAGIC groc_refund.rfnd_dt, rfnd_ind

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Load
# MAGIC ######Truncate and insert strategy. Truncate table, query temporary view created above then insert into schema table.  

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE  ${widget.catalog}.${widget.schema}.ghs_shortfall_voucher;
# MAGIC
# MAGIC INSERT INTO TABLE  ${widget.catalog}.${widget.schema}.ghs_shortfall_voucher
# MAGIC (
# MAGIC     Wallet_ID,
# MAGIC     SPID,
# MAGIC     Loyalty_ID,
# MAGIC     GHS_Order_ID,
# MAGIC     Delivery_Date,
# MAGIC     Refund_Date,
# MAGIC     Rewards_Voucher_Val,
# MAGIC     Credit_Amt,
# MAGIC     Today_Date,
# MAGIC     Credit_Indicator
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     wallet_id as Wallet_ID, 
# MAGIC     singl_profl_id as SPID, 
# MAGIC     loyalty_id as Loyalty_ID, 
# MAGIC     web_order_id as GHS_Order_ID,
# MAGIC     dlvr_dt as Delivery_Date, 
# MAGIC     rfnd_dt as Refund_Date, 
# MAGIC     total_rewards_voucher_val as Rewards_Voucher_Val,
# MAGIC     total_rfnd_amt - web_order_tot_amt as Credit_Amt,
# MAGIC     CURRENT_DATE() as Today_Date,
# MAGIC     Case
# MAGIC     WHen (total_rfnd_amt - web_order_tot_amt) < total_rewards_voucher_val THEN 1 
# MAGIC     Else 0
# MAGIC     End As Credit_Indicator
# MAGIC FROM 
# MAGIC     temp_vwGHS_ShortFall_Voucher
# MAGIC WHERE 
# MAGIC     rfnd_ind = 1 and total_rfnd_amt > web_order_tot_amt
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC ######Count rows from target table  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC count(*) as row_count
# MAGIC FROM ${widget.catalog}.${widget.schema}.ghs_shortfall_voucher
