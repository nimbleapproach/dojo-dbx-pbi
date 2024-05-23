# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: activity_funnel_last_week

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

# MAGIC %md
# MAGIC ###Transformation
# MAGIC ######Query coreprod data into a temporay view 

# COMMAND ----------

import pandas as pd
import numpy as np
import datetime as dt

def get_last_friday():
    now = dt.datetime.now()
    closest_friday = now + dt.timedelta(days=(4 - now.weekday()))
    return (closest_friday if closest_friday < now
            else closest_friday - dt.timedelta(days=7))


last_asda_week = pd.to_datetime(get_last_friday().date(), format='%Y-%m-%d') # for switched funnel

# COMMAND ----------

full_date = str((last_asda_week + dt.timedelta(days=1)).date())
spark.conf.set("a.full_date", full_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_activity_funnel_base
# MAGIC AS
# MAGIC WITH wallet_pos_txns AS ( 
# MAGIC     SELECT
# MAGIC         wallet_id,
# MAGIC         store_nbr,
# MAGIC         cast(event_Ts as date) as event_date
# MAGIC     FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC     WHERE cast(event_Ts as date) >= DATE '2021-09-05'
# MAGIC         AND cast(event_Ts as date) < cast("${a.full_date}" as date)
# MAGIC         --AND store_nbr ~ '^[0-9]+$' -- Use regular expression to filter out test stores
# MAGIC ),
# MAGIC loyalty_acct AS (
# MAGIC     SELECT DISTINCT
# MAGIC         la.wallet_id,
# MAGIC         case when singl_profl_id is not null then singl_profl_id
# MAGIC         when singl_profl_id is null then concat('no_assgnd_spid_', cast   (la.wallet_id as varchar(50)))
# MAGIC         end as singl_profl_id,
# MAGIC         cast(regtn_ts as date) as reg_date,
# MAGIC         acct_status_id,
# MAGIC         CASE WHEN (cast(regtn_ts as date) < date_add(current_date, -37) AND acct_status_id = 'TEMP') THEN 1 ELSE 0 END as acct_del_ind_1
# MAGIC     FROM ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
# MAGIC     LEFT JOIN wallet_pos_txns wpt ON la.wallet_id = wpt.wallet_id AND cast(regtn_ts as date) = wpt.event_date
# MAGIC     WHERE cast(regtn_ts as date) >= DATE '2021-09-05' AND cast(regtn_ts as date) < cast("${a.full_date}" as date)
# MAGIC ),
# MAGIC wallet_scans AS (
# MAGIC     SELECT
# MAGIC         wallet_id,
# MAGIC         MAX(event_date) as most_recent_scan,
# MAGIC         MIN(event_date) as first_scan
# MAGIC     FROM wallet_pos_txns
# MAGIC     WHERE event_date < cast("${a.full_date}" as date)
# MAGIC     GROUP BY wallet_id
# MAGIC ),
# MAGIC wallet_scans_1_day AS (
# MAGIC     SELECT
# MAGIC         wallet_id,
# MAGIC         MAX(event_date) as scan_1_day_ago
# MAGIC     FROM wallet_pos_txns
# MAGIC     WHERE event_date < date_add(cast("${a.full_date}" as date), -1)
# MAGIC     GROUP BY wallet_id
# MAGIC ),
# MAGIC wallet_scans_7_day AS (
# MAGIC     SELECT
# MAGIC         wallet_id,
# MAGIC         MAX(event_date) as scan_7_day_ago
# MAGIC     FROM wallet_pos_txns
# MAGIC     WHERE event_date < date_add(cast("${a.full_date}" as date), -7)
# MAGIC     GROUP BY wallet_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     lyl.singl_profl_id as singl_profl_id,
# MAGIC     lyl.reg_date as regtn_date,
# MAGIC     lyl.wallet_id as wallet_id,
# MAGIC     lyl.acct_del_ind_1 as acct_del_ind_1,
# MAGIC     lyl.acct_status_id as acct_status_id,
# MAGIC     CASE WHEN (cust.mktg_consent_email = 'Y') THEN 1 ELSE 0 END as email_opt_in,
# MAGIC     CASE WHEN (cust.push_notif_consent = 'Y') THEN 1 ELSE 0 END as push_opt_in,
# MAGIC     wallet_scans_1.most_recent_scan as most_recent_scan,
# MAGIC     scan_1_day_ago.scan_1_day_ago as scan_1_day_ago,
# MAGIC     scan_7_day_ago.scan_7_day_ago as scan_7_day_ago,
# MAGIC     wallet_scans_2.first_scan as first_scan
# MAGIC FROM loyalty_acct lyl
# MAGIC LEFT JOIN wallet_scans wallet_scans_1 ON lyl.wallet_id = wallet_scans_1.wallet_id
# MAGIC LEFT JOIN wallet_scans_1_day scan_1_day_ago ON lyl.wallet_id = scan_1_day_ago.wallet_id
# MAGIC LEFT JOIN wallet_scans_7_day scan_7_day_ago ON lyl.wallet_id = scan_7_day_ago.wallet_id
# MAGIC LEFT JOIN wallet_scans wallet_scans_2 ON lyl.wallet_id = wallet_scans_2.wallet_id
# MAGIC LEFT JOIN  ${widget.core_catalog}.gb_customer_data_domain_secured_rpt.cdd_rpt_loyalty_acct
# MAGIC cust ON cust.singl_profl_id = lyl.singl_profl_id
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Load
# MAGIC ######Truncate and insert strategy. Truncate table, query temporary view created above then insert into schema table.

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE  ${widget.catalog}.${widget.schema}.activity_funnel_last_week;
# MAGIC
# MAGIC INSERT INTO TABLE  ${widget.catalog}.${widget.schema}.activity_funnel_last_week
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
# MAGIC
# MAGIC
# MAGIC select 'registered' as type, 1 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date)) as Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_status_id like 'TEMP') as Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT') as Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < date_add(cast("${a.full_date}" as date), -1)) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < date_add(cast("${a.full_date}" as date), -7)) as Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'current accounts' as type, 2 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') as Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < date_add(cast("${a.full_date}" as date), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < date_add(cast("${a.full_date}" as date), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -44)))) as Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'expired accounts' as type, 3 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 1) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 1 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select 0) as Perms
# MAGIC ,
# MAGIC (select 0) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < date_add(cast("${a.full_date}" as date), -38) and acct_del_ind_1 = 1) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < date_add(cast("${a.full_date}" as date), -44) and acct_del_ind_1 = 1) as Total_last_wk
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC
# MAGIC select 'current perms' as type, 4 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') Total
# MAGIC ,
# MAGIC (select 0) Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') as Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'DEFAULT' and regtn_date < date_add(cast("${a.full_date}" as date), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'DEFAULT' and regtn_date < date_add(cast("${a.full_date}" as date), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -44)))) as Total_last_wk
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC
# MAGIC select 'current guests' as type, 5 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select 0) as Perms
# MAGIC ,
# MAGIC (select 0) as Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'TEMP' and regtn_date < date_add(cast("${a.full_date}" as date), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_status_id like 'TEMP' and regtn_date < date_add(cast("${a.full_date}" as date), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -44)))) as Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'distinct_wallet_scans' as type, 6 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago < date_add(cast("${a.full_date}" as date), -1)) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago < date_add(cast("${a.full_date}" as date), -7)) as Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'non expired wallet scans' as type, 7 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan < cast("${a.full_date}" as date) and regtn_date < cast("${a.full_date}" as date) and acct_del_ind_1 = 0 and acct_status_id like 'DEFAULT' and (email_opt_in =1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago < date_add(cast("${a.full_date}" as date), -1) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -38)))) as Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago < date_add(cast("${a.full_date}" as date), -7) and (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -44)))) as Total_last_wk
# MAGIC
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC
# MAGIC select 'wallets, active in last 13 weeks' as type, 8 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7*13) and most_recent_scan < cast("${a.full_date}" as date)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7*13) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7*13) and most_recent_scan <cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7*13) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(cast("${a.full_date}" as date), -92) and scan_1_day_ago < date_add(cast("${a.full_date}" as date), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(cast("${a.full_date}" as date), -98) and scan_7_day_ago < date_add(cast("${a.full_date}" as date), -7)) Total_last_wk
# MAGIC  
# MAGIC union all
# MAGIC
# MAGIC select 'wallets, lapsed' as type, 9 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(cast("${a.full_date}" as date), -7*13)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(cast("${a.full_date}" as date), -7*13) and acct_status_id = 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(cast("${a.full_date}" as date), -7*13) and acct_status_id = 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and most_recent_scan < date_add(cast("${a.full_date}" as date), -7*13) and acct_status_id = 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -38))) and scan_1_day_ago < date_add(cast("${a.full_date}" as date), -92)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -44))) and scan_7_day_ago < date_add(cast("${a.full_date}" as date), -98)) Total_yday
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'wallets, active in last 30 days' as type, 10 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -30) and most_recent_scan < cast("${a.full_date}" as date)) Total --full_date_30_days = full_date - dt.timedelta(days = 30)
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -30) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -30) and most_recent_scan <cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -30) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(cast("${a.full_date}" as date), -31) and scan_1_day_ago < date_add(cast("${a.full_date}" as date), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(cast("${a.full_date}" as date), -37) and scan_7_day_ago < date_add(cast("${a.full_date}" as date), -7)) Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'wallets, active in last 7 days' as type, 11 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7) and most_recent_scan < cast("${a.full_date}" as date)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7) and most_recent_scan <cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -7) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(cast("${a.full_date}" as date), -8) and scan_1_day_ago < date_add(cast("${a.full_date}" as date), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(cast("${a.full_date}" as date), -14) and scan_7_day_ago < date_add(cast("${a.full_date}" as date), -7)) Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC select 'wallets, active in last 1 days' as type, 12 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -1) and most_recent_scan < cast("${a.full_date}" as date)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -1) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'TEMP') Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -1) and most_recent_scan <cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT') Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where most_recent_scan >= date_add(cast("${a.full_date}" as date), -1) and most_recent_scan < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_1_day_ago >= date_add(cast("${a.full_date}" as date), -2) and scan_1_day_ago < date_add(cast("${a.full_date}" as date), -1)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where scan_7_day_ago >= date_add(cast("${a.full_date}" as date), -8) and scan_7_day_ago < date_add(cast("${a.full_date}" as date), -7)) Total_last_wk
# MAGIC
# MAGIC union all
# MAGIC
# MAGIC
# MAGIC select 'RNS - excluding expired guests' as type, 13 as index, 
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_date < cast("${a.full_date}" as date) and (first_scan >= cast("${a.full_date}" as date) or first_scan is null)) Total
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_date < cast("${a.full_date}" as date) and acct_status_id like 'TEMP' and (first_scan >= cast("${a.full_date}" as date) or first_scan is null)) Guests
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_date < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (first_scan >= cast("${a.full_date}" as date) or first_scan is null)) Perms
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where acct_del_ind_1 = 0 and regtn_date < cast("${a.full_date}" as date) and acct_status_id like 'DEFAULT' and (email_opt_in = 1 or push_opt_in = 1) and (first_scan >= cast("${a.full_date}" as date) or first_scan is null)) Perms_opted_in
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -38))) and regtn_date < date_add(cast("${a.full_date}" as date), -1) and ((first_scan >= date_add(cast("${a.full_date}" as date), -1)) or first_scan is null)) Total_yday
# MAGIC ,
# MAGIC (select count(singl_profl_id)
# MAGIC from vw_temp_activity_funnel_base
# MAGIC where (acct_del_ind_1 = 0 or (acct_del_ind_1 = 1 and regtn_date >= date_add(cast("${a.full_date}" as date), -44))) and regtn_date < date_add(cast("${a.full_date}" as date), -7) and ((first_scan >= date_add(cast("${a.full_date}" as date), -7)) or first_scan is null)) Total_last_wk

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC ######Select all from table and see if index 12 toal yesterdays counts are over 75k 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM ${widget.catalog}.${widget.schema}.activity_funnel_last_week
# MAGIC ORDER BY 2

# COMMAND ----------


