# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: activity_funnel_current_week

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

# MAGIC %python
# MAGIC spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
# MAGIC spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
# MAGIC spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
# MAGIC
# MAGIC #Reset the widgets values to avoid any caching issues. 
# MAGIC dbutils.widgets.text('core_catalog', "")
# MAGIC dbutils.widgets.text('catalog', "")
# MAGIC dbutils.widgets.text('schema', "")
# MAGIC dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation
# MAGIC ######Query coreprod data into a temporay view  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_activity_funnel_base
# MAGIC AS
# MAGIC
# MAGIC WITH wallet_pos_txns AS (
# MAGIC         select wallet_id
# MAGIC         ,store_nbr
# MAGIC         ,event_ts
# MAGIC
# MAGIC     from ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     where 1=1
# MAGIC     and cast(event_ts as date) < current_date()
# MAGIC     and cast(event_ts as date) >= cast('2021-09-05' as date)
# MAGIC )
# MAGIC
# MAGIC , loyalty_acct as (
# MAGIC
# MAGIC         select distinct la.wallet_id
# MAGIC             ,case when singl_profl_id is not null then singl_profl_id
# MAGIC             when singl_profl_id is null then concat('no_assgnd_spid_', cast   (la.wallet_id as varchar(50)))
# MAGIC             end as singl_profl_id
# MAGIC             ,cast(regtn_ts as date) as reg_dt
# MAGIC             ,acct_status_id
# MAGIC             ,regtn_ts
# MAGIC             ,case when (cast(regtn_ts as date) < date_add(current_date(), -37) and acct_status_id='TEMP') then 1 else 0
# MAGIC             end as acct_del_ind_1
# MAGIC         from ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC         left join ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC         on la.wallet_id=wpt.wallet_id
# MAGIC         and cast(la.regtn_ts as date)=cast(wpt.event_ts as date)
# MAGIC         where 1=1
# MAGIC         and cast(regtn_ts as date) < current_date() and cast(regtn_ts as date) >= cast('2021-09-05' as date))
# MAGIC
# MAGIC SELECT 
# MAGIC     lyl.singl_profl_id as singl_profl_id
# MAGIC     ,cast(regtn_ts as date) regtn_ts
# MAGIC     ,lyl.wallet_id as wallet_id
# MAGIC     ,lyl.acct_del_ind_1 as acct_del_ind_1
# MAGIC     ,lyl.acct_status_id as acct_status_id
# MAGIC     ,case when (cust.mktg_consent_email = 'Y') then 1 else 0 end as email_opt_in
# MAGIC     ,case when (cust.push_notif_consent = 'Y') then 1 else 0 end as push_opt_in
# MAGIC     ,cast(most_recent_scan.most_recent_scan as date) as most_recent_scan
# MAGIC     ,cast(scan_1_day_ago.scan_1_day_ago as date) as scan_1_day_ago
# MAGIC     ,cast(scan_7_day_ago.scan_7_day_ago as date) as scan_7_day_ago
# MAGIC     ,cast(first_scan.first_scan as date) as first_scan
# MAGIC     
# MAGIC FROM  
# MAGIC     (SELECT wallet_id, max(event_ts) as most_recent_scan
# MAGIC             FROM wallet_pos_txns  wal 
# MAGIC             WHERE event_ts < current_date()
# MAGIC             GROUP BY wallet_id) most_recent_scan
# MAGIC INNER JOIN
# MAGIC     (SELECT wallet_id, min(event_ts) as first_scan
# MAGIC             FROM wallet_pos_txns  wal 
# MAGIC             WHERE event_ts < current_date()
# MAGIC             GROUP BY wallet_id) first_scan
# MAGIC ON most_recent_scan.wallet_id = first_scan.wallet_id
# MAGIC LEFT OUTER JOIN   
# MAGIC     (SELECT wallet_id, max(event_ts) as scan_1_day_ago
# MAGIC         FROM wallet_pos_txns  wal
# MAGIC             WHERE event_ts <DATE_ADD(current_date(),-1) 
# MAGIC             GROUP BY wallet_id) scan_1_day_ago
# MAGIC     ON most_recent_scan.wallet_id = scan_1_day_ago.wallet_id
# MAGIC LEFT OUTER JOIN
# MAGIC     (SELECT wallet_id, max(event_ts) as scan_7_day_ago
# MAGIC             FROM wallet_pos_txns  wal
# MAGIC             WHERE wal.event_ts <DATE_ADD(current_date() ,-7) 
# MAGIC             GROUP BY wallet_id) scan_7_day_ago
# MAGIC     ON most_recent_scan.wallet_id = scan_7_day_ago.wallet_id
# MAGIC RIGHT OUTER JOIN
# MAGIC     loyalty_acct lyl 
# MAGIC     ON lyl.wallet_id = most_recent_scan.wallet_id
# MAGIC LEFT OUTER JOIN 
# MAGIC    ${widget.core_catalog}.gb_customer_data_domain_secured_rpt.cdd_rpt_loyalty_acct cust
# MAGIC     ON cust.singl_profl_id = lyl.singl_profl_id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Load
# MAGIC ######Truncate and insert strategy. Truncate table, query temporary view created above then insert into schema table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC TRUNCATE TABLE  ${widget.catalog}.${widget.schema}.activity_funnel_current_week;
# MAGIC
# MAGIC INSERT INTO TABLE  ${widget.catalog}.${widget.schema}.activity_funnel_current_week
# MAGIC
# MAGIC (
# MAGIC     type,
# MAGIC     index,
# MAGIC     Total,
# MAGIC     Guests,
# MAGIC     Perms,
# MAGIC     Perms_opted_in,
# MAGIC     Total_yday,
# MAGIC     Total_last_wk
# MAGIC )
# MAGIC
# MAGIC SELECT 'registered' as type, 1 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date()) as Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_status_id like 'TEMP') as Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_status_id like 'DEFAULT') as Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < date_add(current_date(), -1)) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < date_add(current_date(), -7)) as Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'current accounts' as type, 2 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') as Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < date_add(current_date(), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < date_add(current_date(), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -44)))) as Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'expired accounts' as type, 3 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 1) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 1 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select 0) as Perms
# MAGIC ,
# MAGIC (select 0) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < date_add(current_date(), -38) and acct_del_ind_1 = 1) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < date_add(current_date(), -44) and acct_del_ind_1 = 1) as Total_last_wk
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC
# MAGIC select 'current perms' as type, 4 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') Total
# MAGIC ,
# MAGIC (select 0) Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') as Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'DEFAULT' and regtn_ts < date_add(current_date(), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'DEFAULT' and regtn_ts < date_add(current_date(), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -44)))) as Total_last_wk
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC select 'current guests' as type, 5 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select 0) as Perms
# MAGIC ,
# MAGIC (select 0) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'TEMP' and regtn_ts < date_add(current_date(), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'TEMP' and regtn_ts < date_add(current_date(), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -44)))) as Total_last_wk
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'distinct_wallet_scans' as type, 6 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date()) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago < date_add(current_date(), -1)) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago < date_add(current_date(), -7)) as Total_last_wk
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'non expired wallet scans' as type, 7 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and regtn_ts < current_date() and acct_del_ind_1 = 0) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < current_date() and regtn_ts < current_date() and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago < date_add(current_date(), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago < date_add(current_date(), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -44)))) as Total_last_wk
# MAGIC
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC select 'wallets, active in last 13 weeks' as type, 8 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7*13) and most_recent_scan < current_date()) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7*13) and most_recent_scan < current_date() and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7*13) and most_recent_scan <current_date() and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7*13) and most_recent_scan < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(current_date(), -92) and scan_1_day_ago < date_add(current_date(), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(current_date(), -98) and scan_7_day_ago < date_add(current_date(), -7)) Total_last_wk
# MAGIC  
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'wallets, lapsed' as type, 9 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(current_date(), -7*13)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(current_date(), -7*13) and acct_status_id = 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(current_date(), -7*13) and acct_status_id = 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(current_date(), -7*13) and acct_status_id = 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -38))) and scan_1_day_ago < date_add(current_date(), -92)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -44))) and scan_7_day_ago < date_add(current_date(), -98)) Total_yday
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'wallets, active in last 30 days' as type, 10 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -30) and most_recent_scan < current_date()) Total --full_date_30_days = full_date - dt.timedelta(days = 30)
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -30) and most_recent_scan < current_date() and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -30) and most_recent_scan <current_date() and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -30) and most_recent_scan < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(current_date(), -31) and scan_1_day_ago < date_add(current_date(), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(current_date(), -37) and scan_7_day_ago < date_add(current_date(), -7)) Total_last_wk
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'wallets, active in last 7 days' as type, 11 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7) and most_recent_scan < current_date()) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7) and most_recent_scan < current_date() and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7) and most_recent_scan <current_date() and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -7) and most_recent_scan < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(current_date(), -8) and scan_1_day_ago < date_add(current_date(), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(current_date(), -14) and scan_7_day_ago < date_add(current_date(), -7)) Total_last_wk
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select 'wallets, active in last 1 days' as type, 12 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -1) and most_recent_scan < current_date()) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -1) and most_recent_scan < current_date() and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -1) and most_recent_scan <current_date() and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(current_date(), -1) and most_recent_scan < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(current_date(), -2) and scan_1_day_ago < date_add(current_date(), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(current_date(), -8) and scan_7_day_ago < date_add(current_date(), -7)) Total_last_wk
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC
# MAGIC select 'RNS - excluding expired guests' as type, 13 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_ts < current_date() and (first_scan >= current_date() or first_scan is null)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_ts < current_date() and acct_status_id like 'TEMP' and (first_scan >= current_date() or first_scan is null)) Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_ts < current_date() and acct_status_id like 'DEFAULT' and (first_scan >= current_date() or first_scan is null)) Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_ts < current_date() and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1) and (first_scan >= current_date() or first_scan is null)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -38))) and regtn_ts < date_add(current_date(), -1) and ((first_scan >= date_add(current_date(), -1)) or first_scan is null)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_ts >= date_add(current_date(), -44))) and regtn_ts < date_add(current_date(), -7) and ((first_scan >= date_add(current_date(), -7)) or first_scan is null)) Total_last_wk

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC ######Select all from table and see if index 12 toal yesterdays counts are over 75k 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM ${widget.catalog}.${widget.schema}.activity_funnel_current_week
# MAGIC ORDER BY 2
# MAGIC
