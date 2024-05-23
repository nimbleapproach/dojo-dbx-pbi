# Databricks notebook source
# MAGIC %md
# MAGIC ##Process:     nursery_time_series

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 

# COMMAND ----------

core_catalog = dbutils.widgets.get("core_catalog")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

display(spark.sql(f"""SELECT '{core_catalog}' AS core_catalog,'{catalog}' AS catalog,'{schema}' AS schema;"""))

# COMMAND ----------

import datetime
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ###Assign a cutoff date for delta load

# COMMAND ----------

cutoff_dt = ((datetime.datetime.today() - relativedelta(months=1)).strftime('%Y-%m-%d'))
display(cutoff_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the count of nursery_time_series table

# COMMAND ----------

df_act_cnt = spark.table(f"{catalog}.{schema}.nursery_time_series")
tbl_cnt = df_act_cnt.count()
display(tbl_cnt)



# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the count of missing dates in active earners table

# COMMAND ----------

missing_days = spark.sql(f"""
                            select distinct day_date from coreprod.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar a
                            left join {catalog}.{schema}.nursery_time_series b
                            on a.day_date = b.full_date
                            where b.full_date is null
                            /* 2021-09-07 is the minimum date for which the data is currently present in the table */
                            and a.day_date>='2021-09-07' and a.day_date <= "{cutoff_dt}";
                        """)
missing_cnt = missing_days.count()
display(missing_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Condition based data load
# MAGIC #####If table has no records or data is missing, then load the entire history from 2021 else replace data from cutoff date

# COMMAND ----------

if (tbl_cnt == 0 or missing_cnt > 0):
    print("Loading full table as the data was missing or table was empty")
    df = spark.sql(f"""SELECT day_date as full_date
                FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
                WHERE day_date >= '2021-09-07'
                AND day_date <= current_date();""")
else:
    print("Loading data from last one month")
    df = spark.sql(f"""SELECT day_date as full_date
                FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
                WHERE day_date > "{cutoff_dt}"
                AND day_date <= current_date();""")
    
df.createOrReplaceTempView("dt")


# COMMAND ----------

spark.sql(f"""
          
            CREATE OR REPLACE TEMPORARY VIEW vw_temp_wallet_pos_txns
            AS
            SELECT wallet_id
            ,store_nbr
            ,event_ts
            ,d.full_date
            from {core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
            join dt d
            on cast(wpt.event_ts as date) >=  '2021-09-05'
            and cast(event_ts as date) <= d.full_date
      """)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_lapsed
# MAGIC AS
# MAGIC
# MAGIC with cte_lapsed
# MAGIC as
# MAGIC (select distinct wp.full_date, wallet_id, max(cast(event_ts as date)) as last_scn
# MAGIC from vw_temp_wallet_pos_txns wp
# MAGIC group by  wp.full_date, wallet_id
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select dateadd(cte.full_date, -1) as full_date, count(*) as lapsed from cte_lapsed cte
# MAGIC where datediff(cte.full_date, last_scn) > 92
# MAGIC group by dateadd(cte.full_date, -1)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_reactive
# MAGIC AS
# MAGIC
# MAGIC with cte_reactive
# MAGIC AS
# MAGIC (
# MAGIC     SELECT 
# MAGIC     full_date
# MAGIC     ,wallet_id
# MAGIC     ,cast(event_ts as date) as dt
# MAGIC     ,lead(event_ts,1) over(PARTITION BY walleT_id order by cast(event_ts as date) desc) as prv_dt
# MAGIC     , datediff( cast(EVENT_TS as date), lead(event_ts,1) over(PARTITION BY walleT_id order by cast(event_ts as date) desc)) as diff
# MAGIC     FROM vw_temp_wallet_pos_txns
# MAGIC )
# MAGIC
# MAGIC SELECT dateadd(dt.full_date, -1) as full_date, count(*) as reactivated
# MAGIC FROM cte_reactive r
# MAGIC JOIN dt
# MAGIC ON r.dt = dateadd(dt.full_date, -1)
# MAGIC and diff > 92
# MAGIC group by dateadd(dt.full_date, -1)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_nursery
# MAGIC AS
# MAGIC
# MAGIC WITH cte_nursery
# MAGIC AS 
# MAGIC (
# MAGIC SELECT full_date, wallet_id, count(*) as nm_scn, max(cast(event_ts as date)) as lst_scn
# MAGIC FROM vw_temp_wallet_pos_txns
# MAGIC GROUP BY full_date, wallet_id
# MAGIC )
# MAGIC SELECT dateadd(full_date, -1) as full_date, count(*) as nursery
# MAGIC FROM cte_nursery 
# MAGIC WHERE nm_scn <4
# MAGIC AND Datediff(full_date,lst_scn) <=30
# MAGIC GROUP BY dateadd(full_date, -1)

# COMMAND ----------

active_df = spark.sql(f"""
                SELECT d.full_date as full_date, reactivated, lapsed, nursery 
                FROM dt d
                LEFT JOIN vw_lapsed l on d.full_date = l.full_date
                LEFT JOIN vw_reactive r on d.full_date = r.full_date
                LEFT JOIN vw_nursery n on d.full_date = n.full_date
                WHERE reactivated is not null or lapsed is not null or nursery is not null
            """)
active_df.createOrReplaceTempView("nursery_time_series")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading data from temporary views into main table

# COMMAND ----------

spark.sql(f"""
          
            MERGE INTO {catalog}.{schema}.nursery_time_series AS TARGET
            USING nursery_time_series AS SOURCE
            ON SOURCE.full_date = TARGET.full_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *;
             
            """)

