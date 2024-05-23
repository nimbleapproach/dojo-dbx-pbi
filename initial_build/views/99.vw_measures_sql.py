# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_measures_sql";

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

# COMMAND ------------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC with end_date as (select end_date from ${widget.catalog}.${widget.schema}.vw_last_asda_week)
# MAGIC
# MAGIC select 
# MAGIC     register_never_scanned_24_hrs as register_never_scanned_24_hrs
# MAGIC     ,register_never_scanned_7_days as register_never_scanned_7_days
# MAGIC     ,t as avg_time_voucher_held_mins
# MAGIC     ,num_wallet_id as num_wallet_id , new_spid_opt_in as new_spid_opt_in
# MAGIC     ,num_wallet_id_last_wk as num_wallet_id_last_wk
# MAGIC     ,register_never_scanned_24_hrs_last_wk as register_never_scanned_24_hrs_last_wk
# MAGIC     ,register_never_scanned_7_days_last_wk as register_never_scanned_7_days_last_wk
# MAGIC from
# MAGIC
# MAGIC /*Measure 1
# MAGIC This subquery shows the number of people wh have signed up but not scanned in the past 1 / 7 days, based on wallet_pos_txn
# MAGIC */
# MAGIC     (select register_never_scanned_24_hrs, register_never_scanned_7_days, x.joinc
# MAGIC     from
# MAGIC     -- count of sign ups in the last 24h, i.e. when signup date is the maximum value,  where the spid does not appear in wallet_pos
# MAGIC         (select count(*) as register_never_scanned_24_hrs, 1 as joinc
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_sign_ups
# MAGIC         where marvel_signup_dt = (select max(marvel_signup_dt) from ${widget.catalog}.${widget.schema}.vw_sign_ups) 
# MAGIC         and singl_profl_id not in(
# MAGIC             -- wallet_pos joins to loyalty_acct to bring in SPID
# MAGIC             select l.singl_profl_id
# MAGIC             from ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns w
# MAGIC             join ${widget.catalog}.${widget.schema}.vw_loyalty_acct l
# MAGIC             on w.wallet_id=l.wallet_id)
# MAGIC         ) x
# MAGIC     join
# MAGIC     --Similar count, but for 7 days
# MAGIC         (select count(*) as register_never_scanned_7_days, 1 as joinc 
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_sign_ups
# MAGIC         where (marvel_signup_dt 
# MAGIC         -- Use BETWEEN to find all records where signup is between two dates
# MAGIC             between 
# MAGIC                 (select DATE_ADD(max(marvel_signup_dt), -6) from ${widget.catalog}.${widget.schema}.vw_sign_ups) 
# MAGIC             and (select max(marvel_signup_dt) 
# MAGIC                 from ${widget.catalog}.${widget.schema}.vw_sign_ups))
# MAGIC         and singl_profl_id not in(
# MAGIC             -- wallet_pos joins to loyalty_acct to bring in SPID
# MAGIC             select l.singl_profl_id
# MAGIC             from ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns w
# MAGIC             join ${widget.catalog}.${widget.schema}.vw_loyalty_acct l
# MAGIC             on w.wallet_id=l.wallet_id)
# MAGIC         ) y
# MAGIC     on x.joinc=y.joinc) f
# MAGIC
# MAGIC /*Measure 2
# MAGIC This shows the average time a voucher has been held, in minutes
# MAGIC */
# MAGIC JOIN
# MAGIC     (
# MAGIC         select t
# MAGIC         ,1 as joinc
# MAGIC     from
# MAGIC         (
# MAGIC         select t
# MAGIC         ,ROW_NUMBER() OVER(ORDER BY t asc) as ind
# MAGIC         from
# MAGIC         -- Calculate the average time between voucher gained / redeemed
# MAGIC         -- * 0.0166667 is to convert it to minutes
# MAGIC         -- Marina didn't want to do /60 for some reason
# MAGIC         -- I didn't argue
# MAGIC             (select *, timestampdiff(second,reward_gained_ts, reward_redm_ts)*(0.0166667) as t
# MAGIC             from ${widget.catalog}.${widget.schema}.vw_reward_wallets
# MAGIC             where reward_redm_ts is not null
# MAGIC             order by t asc
# MAGIC             ))
# MAGIC     -- The below where clause ranks the times held till redemption, then takes the max()/2 ranking
# MAGIC     -- This means we show  the median time held, not mean
# MAGIC     where ind in 
# MAGIC         (select 
# MAGIC             max(ind)/2 as g from (select t
# MAGIC             ,ROW_NUMBER() OVER(ORDER BY t asc) as ind
# MAGIC         from
# MAGIC             (select *, timestampdiff(second,reward_gained_ts, reward_redm_ts)*(0.0166667) as t
# MAGIC             from ${widget.catalog}.${widget.schema}.vw_reward_wallets
# MAGIC             where reward_redm_ts is not null
# MAGIC             order by t asc))
# MAGIC     )
# MAGIC     )g
# MAGIC on f.joinc=g.joinc
# MAGIC
# MAGIC /*
# MAGIC Measure 3 The number of distinct wallet_ids within wallet_pos_txns
# MAGIC */
# MAGIC join
# MAGIC     (select 
# MAGIC         count(distinct wallet_id) as num_wallet_id
# MAGIC         ,'1' as joinr 
# MAGIC     from ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns) r
# MAGIC on r.joinr=g.joinc
# MAGIC
# MAGIC /*
# MAGIC Measure 4
# MAGIC This shows the new spid opt in rate
# MAGIC */
# MAGIC join
# MAGIC     (select 
# MAGIC         cast(sum(opt_in)as float)/(cast(sum(new_ind_1) as float)) as new_spid_opt_in
# MAGIC         ,1 as joinh 
# MAGIC     from
# MAGIC     -- When the account is a perm account and the opt in flag is 1, then 1 else 0 for opt_in
# MAGIC     -- When the account is a perm account and the new_ind flag is 1 then 1 else 0 for new_ind
# MAGIC         (select case when (acct_status_id = 'DEFAULT' and opt_in_flag=1) then 1 else 0 end as opt_in 
# MAGIC             , case when (acct_status_id = 'DEFAULT' and new_ind=1) then 1 else 0 end as new_ind_1
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_sign_ups)
# MAGIC     )h
# MAGIC on f.joinc=h.joinh
# MAGIC
# MAGIC /*
# MAGIC Measure 5
# MAGIC Calculates the number of distinct wallet_ids in wallet_pos_txn where the event_ts is <= the friday ending the most recent completed asda week
# MAGIC */
# MAGIC join
# MAGIC     (
# MAGIC     select count(distinct wallet_id) num_wallet_id_last_wk, '1' as join_w_lwk
# MAGIC     from ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns
# MAGIC     where event_ts <= (select * FROM end_date)    
# MAGIC     )k
# MAGIC on 
# MAGIC r.joinr = k.join_w_lwk
# MAGIC
# MAGIC /*
# MAGIC Measure 6
# MAGIC Calculate the number of records in sign_ups where the SPID does not appear in wallet_pos_txn 
# MAGIC AND the user signed up on Friday of the prior asda week
# MAGIC */
# MAGIC join
# MAGIC     (
# MAGIC     select 
# MAGIC         count(*) as register_never_scanned_24_hrs_last_wk
# MAGIC         ,1 as joinc
# MAGIC     from ${widget.catalog}.${widget.schema}.vw_sign_ups
# MAGIC     where marvel_signup_dt = (select * from end_date) 
# MAGIC     and singl_profl_id not in 
# MAGIC         (select l.singl_profl_id
# MAGIC         -- Join to loyalty_acct to bring in SPID
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns w
# MAGIC         join ${widget.catalog}.${widget.schema}.vw_loyalty_acct l
# MAGIC         on w.wallet_id=l.wallet_id
# MAGIC         -- AND w.event_ts <= (select * FROM end_date)
# MAGIC         )
# MAGIC     ) p
# MAGIC on r.joinr = p.joinc
# MAGIC
# MAGIC /*
# MAGIC Measure 7
# MAGIC Calculate the number of people who signed up in the 7 days leading up to the friday of the most recent asda week 
# MAGIC AND 
# MAGIC Who didnt appear in the wallet_pos_txn before then
# MAGIC */
# MAGIC join
# MAGIC     (
# MAGIC     select 
# MAGIC         count(*) as register_never_scanned_7_days_last_wk
# MAGIC         ,1 as joinc 
# MAGIC     from ${widget.catalog}.${widget.schema}.vw_sign_ups
# MAGIC     where (marvel_signup_dt 
# MAGIC         between (select DATE_ADD(end_date, -6) from ${widget.catalog}.${widget.schema}.vw_last_asda_week) 
# MAGIC         and (select * from end_date))
# MAGIC     and singl_profl_id not in(
# MAGIC         select l.singl_profl_id
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns w
# MAGIC         join ${widget.catalog}.${widget.schema}.vw_loyalty_acct l
# MAGIC         on w.wallet_id=l.wallet_id
# MAGIC         -- AND w.event_ts <= (select * FROM end_date)
# MAGIC     )) q
# MAGIC     on 
# MAGIC r.joinr = q.joinc
# MAGIC
# MAGIC
# MAGIC
# MAGIC
