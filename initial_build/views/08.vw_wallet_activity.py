# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_wallet_activity";
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
# MAGIC with dt_lookup as (
# MAGIC         -- For each asda year, find the start and end date and apply a flag = 1 for the current year
# MAGIC         --This can be used to flag if an event was in the current YTD
# MAGIC     SELECT  asda_year, start_dt
# MAGIC         ,coalesce(DATE_ADD(lead(start_dt,1) OVER (ORDER BY asda_year asc ),-1), cast('2099-12-31' as date)) end_dt
# MAGIC         ,CASE WHEN current_date between start_dt and DATE_ADD(lead(start_dt,1) OVER (ORDER BY asda_year asc ),-1) THEN 1 ELSE 0 end as yr_flag
# MAGIC     FROM (
# MAGIC         select  asda_year
# MAGIC                 ,min(day_date) start_dt
# MAGIC         FROM ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
# MAGIC         GROUP BY asda_year
# MAGIC         order by asda_year asc
# MAGIC         )
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     --  DISTINCT EVENT_TS
# MAGIC     DISTINCT WALLET_ID
# MAGIC
# MAGIC     --The MAX statement here means each wallet will only have a single record in the final table
# MAGIC     ,MAX(CURRENT_1_DAY_ACTIVE_FLAG) CURRENT_1_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(CURRENT_7_DAY_ACTIVE_FLAG) CURRENT_7_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(CURRENT_30_DAY_ACTIVE_FLAG) CURRENT_30_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(CURRENT_91_DAY_ACTIVE_FLAG) CURRENT_91_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(YESTERDAY_1_DAY_ACTIVE_FLAG) YESTERDAY_1_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(YESTERDAY_7_DAY_ACTIVE_FLAG) YESTERDAY_7_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(YESTERDAY_30_DAY_ACTIVE_FLAG) YESTERDAY_30_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(YESTERDAY_91_DAY_ACTIVE_FLAG) YESTERDAY_91_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(ROLLING_1_DAY_ACTIVE_FLAG) ROLLING_1_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(ROLLING_7_DAY_ACTIVE_FLAG) ROLLING_7_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(ROLLING_30_DAY_ACTIVE_FLAG) ROLLING_30_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(ROLLING_91_DAY_ACTIVE_FLAG) ROLLING_91_DAY_ACTIVE_FLAG
# MAGIC     ,MAX(yr_flag) as YTD_FLAG
# MAGIC FROM (
# MAGIC     SELECT  wallet_id
# MAGIC             ,EVENT_TS
# MAGIC
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -1)  AND CURRENT_DATE THEN  1 ELSE 0 END CURRENT_1_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -7)  AND CURRENT_DATE THEN  1 ELSE 0 END CURRENT_7_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -30)  AND CURRENT_DATE THEN  1 ELSE 0 END CURRENT_30_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -91)  AND CURRENT_DATE THEN  1 ELSE 0 END CURRENT_91_DAY_ACTIVE_FLAG
# MAGIC
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -2) AND DATE_ADD(CURRENT_DATE, -2) THEN 1 ELSE 0 END YESTERDAY_1_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -8) AND DATE_ADD(CURRENT_DATE, -2) THEN 1 ELSE 0 END YESTERDAY_7_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -31) AND DATE_ADD(CURRENT_DATE, -2) THEN 1 ELSE 0 END YESTERDAY_30_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -92) AND DATE_ADD(CURRENT_DATE, -2) THEN 1 ELSE 0 END YESTERDAY_91_DAY_ACTIVE_FLAG
# MAGIC
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -8) AND DATE_ADD(CURRENT_DATE, -8) THEN 1 ELSE 0 END ROLLING_1_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -14) AND DATE_ADD(CURRENT_DATE, -8) THEN 1 ELSE 0 END ROLLING_7_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -37) AND DATE_ADD(CURRENT_DATE, -8) THEN 1 ELSE 0 END ROLLING_30_DAY_ACTIVE_FLAG
# MAGIC             ,CASE WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -98) AND DATE_ADD(CURRENT_DATE, -8) THEN 1 ELSE 0 END ROLLING_91_DAY_ACTIVE_FLAG
# MAGIC     FROM 
# MAGIC         (SELECT WALLET_ID, CAST(EVENT_TS AS DATE) EVENT_TS 
# MAGIC         FROM ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt 
# MAGIC         WHERE 1=1
# MAGIC         --filter out test stores
# MAGIC         AND store_nbr like '^-?\d*$'
# MAGIC         AND CAST(event_ts AS DATE) < CURRENT_DATE
# MAGIC         --only show transactions from the first launch of rewards
# MAGIC         and cast(event_ts as date) >= cast('2021-09-05' as date))
# MAGIC     ) a
# MAGIC
# MAGIC  -- Bring in flag to show if event was in YTD   
# MAGIC LEFT OUTER JOIN dt_lookup
# MAGIC     ON cast(a.event_ts as date) between dt_lookup.start_dt and dt_lookup.end_dt
# MAGIC GROUP BY 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC
# MAGIC with dt_lookup as (
# MAGIC   -- For each asda year, find the start and end date and apply a flag = 1 for the current year
# MAGIC   --This can be used to flag if an event was in the current YTD
# MAGIC   SELECT
# MAGIC     asda_year,
# MAGIC     start_dt,
# MAGIC     coalesce(
# MAGIC       DATE_ADD(
# MAGIC         lead(start_dt, 1) OVER (
# MAGIC           ORDER BY
# MAGIC             asda_year asc
# MAGIC         ),
# MAGIC         -1
# MAGIC       ),
# MAGIC       cast('2099-12-31' as date)
# MAGIC     ) end_dt,CASE
# MAGIC       WHEN current_date between start_dt
# MAGIC       and DATE_ADD(
# MAGIC         lead(start_dt, 1) OVER (
# MAGIC           ORDER BY
# MAGIC             asda_year asc
# MAGIC         ),
# MAGIC         -1
# MAGIC       ) THEN 1
# MAGIC       ELSE 0
# MAGIC     end as yr_flag
# MAGIC   FROM
# MAGIC     (
# MAGIC       select
# MAGIC         asda_year,
# MAGIC         min(day_date) start_dt
# MAGIC       FROM
# MAGIC         ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
# MAGIC       GROUP BY
# MAGIC         asda_year
# MAGIC       order by
# MAGIC         asda_year asc
# MAGIC     )
# MAGIC )
# MAGIC SELECT
# MAGIC   --  DISTINCT EVENT_TS
# MAGIC   DISTINCT WALLET_ID --The MAX statement here means each wallet will only have a single record in the final table
# MAGIC ,
# MAGIC   MAX(CURRENT_1_DAY_ACTIVE_FLAG) CURRENT_1_DAY_ACTIVE_FLAG,
# MAGIC   MAX(CURRENT_7_DAY_ACTIVE_FLAG) CURRENT_7_DAY_ACTIVE_FLAG,
# MAGIC   MAX(CURRENT_30_DAY_ACTIVE_FLAG) CURRENT_30_DAY_ACTIVE_FLAG,
# MAGIC   MAX(CURRENT_91_DAY_ACTIVE_FLAG) CURRENT_91_DAY_ACTIVE_FLAG,
# MAGIC   MAX(YESTERDAY_1_DAY_ACTIVE_FLAG) YESTERDAY_1_DAY_ACTIVE_FLAG,
# MAGIC   MAX(YESTERDAY_7_DAY_ACTIVE_FLAG) YESTERDAY_7_DAY_ACTIVE_FLAG,
# MAGIC   MAX(YESTERDAY_30_DAY_ACTIVE_FLAG) YESTERDAY_30_DAY_ACTIVE_FLAG,
# MAGIC   MAX(YESTERDAY_91_DAY_ACTIVE_FLAG) YESTERDAY_91_DAY_ACTIVE_FLAG,
# MAGIC   MAX(ROLLING_1_DAY_ACTIVE_FLAG) ROLLING_1_DAY_ACTIVE_FLAG,
# MAGIC   MAX(ROLLING_7_DAY_ACTIVE_FLAG) ROLLING_7_DAY_ACTIVE_FLAG,
# MAGIC   MAX(ROLLING_30_DAY_ACTIVE_FLAG) ROLLING_30_DAY_ACTIVE_FLAG,
# MAGIC   MAX(ROLLING_91_DAY_ACTIVE_FLAG) ROLLING_91_DAY_ACTIVE_FLAG,
# MAGIC   MAX(yr_flag) as YTD_FLAG
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       wallet_id,
# MAGIC       EVENT_TS
# MAGIC       /* 
# MAGIC       Current_date was originally included during the build phase because I'm an idiot who kept forgetting what the date was.
# MAGIC                   ,current_date
# MAGIC       For each of the below, the logic is "is this event between day X and day Y", with a flag set to 1 if it is.
# MAGIC       */,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -1)
# MAGIC         AND CURRENT_DATE THEN 1
# MAGIC         ELSE 0
# MAGIC       END CURRENT_1_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -7)
# MAGIC         AND CURRENT_DATE THEN 1
# MAGIC         ELSE 0
# MAGIC       END CURRENT_7_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -30)
# MAGIC         AND CURRENT_DATE THEN 1
# MAGIC         ELSE 0
# MAGIC       END CURRENT_30_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -91)
# MAGIC         AND CURRENT_DATE THEN 1
# MAGIC         ELSE 0
# MAGIC       END CURRENT_91_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -2)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -2) THEN 1
# MAGIC         ELSE 0
# MAGIC       END YESTERDAY_1_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -8)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -2) THEN 1
# MAGIC         ELSE 0
# MAGIC       END YESTERDAY_7_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -31)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -2) THEN 1
# MAGIC         ELSE 0
# MAGIC       END YESTERDAY_30_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -92)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -2) THEN 1
# MAGIC         ELSE 0
# MAGIC       END YESTERDAY_91_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -8)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -8) THEN 1
# MAGIC         ELSE 0
# MAGIC       END ROLLING_1_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -14)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -8) THEN 1
# MAGIC         ELSE 0
# MAGIC       END ROLLING_7_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -37)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -8) THEN 1
# MAGIC         ELSE 0
# MAGIC       END ROLLING_30_DAY_ACTIVE_FLAG,CASE
# MAGIC         WHEN EVENT_TS BETWEEN DATE_ADD(CURRENT_DATE, -98)
# MAGIC         AND DATE_ADD(CURRENT_DATE, -8) THEN 1
# MAGIC         ELSE 0
# MAGIC       END ROLLING_91_DAY_ACTIVE_FLAG
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           WALLET_ID,
# MAGIC           CAST(EVENT_TS AS DATE) EVENT_TS
# MAGIC         FROM
# MAGIC           ${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
# MAGIC         WHERE
# MAGIC           1 = 1 --filter out test stores
# MAGIC           AND store_nbr like '^-?\d*$'
# MAGIC           AND CAST(event_ts AS DATE) < CURRENT_DATE --only show transactions from the first launch of rewards
# MAGIC           and cast(event_ts as date) >= cast('2021-09-05' as date)
# MAGIC       )
# MAGIC   ) a -- Bring in flag to show if event was in YTD
# MAGIC   LEFT OUTER JOIN dt_lookup ON cast(a.event_ts as date) between dt_lookup.start_dt
# MAGIC   and dt_lookup.end_dt
# MAGIC GROUP BY
# MAGIC   1;
