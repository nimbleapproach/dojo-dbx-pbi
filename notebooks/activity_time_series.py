# Databricks notebook source
# MAGIC %md
# MAGIC ##Process:     activity_time_series

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
# MAGIC ###Get the count of ativity time series table

# COMMAND ----------

df_act_cnt = spark.table(f"{catalog}.{schema}.activity_time_series")
tbl_cnt = df_act_cnt.count()
display(tbl_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the count of missing dates in active earners table

# COMMAND ----------

missing_days = spark.sql(f"""
                            select distinct day_date from coreprod.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar a
                            left join {catalog}.{schema}.activity_time_series b
                            on a.day_date = b.full_date
                            where b.full_date is null
                            /* 2022-08-01 is the minimum date for which the data is currently present in the table */
                            and a.day_date>= '2022-08-01' and a.day_date <= "{cutoff_dt}";
                        """)
missing_cnt = missing_days.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Condition based data load
# MAGIC #####If table has no records or data is missing, then load the entire history from 2021 else replace data from cutoff date

# COMMAND ----------

if (tbl_cnt == 0 or missing_cnt > 0):
    print("Loading full table as the data was missing or table was empty")
    active_df = spark.sql(f"""
                          with cte as
                          (
                            SELECT day_date as full_date, date_add(day_date, -1) as prev_day, date_add(day_date, -7) as prev_7, date_add(day_date, -30) as prev_30,
                            date_add(day_date, -91) as prev_91
                            FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
                            where day_date >= '2022-08-01' and day_date <= current_date()
                            )
 
                            SELECT 
                            full_date 
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM coreprod.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) = prev_day
                            ) active_prev_d
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM coreprod.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) between prev_7 and prev_day
                            ) active_prev_7d
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM coreprod.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) between prev_30 and prev_day
                            ) active_prev_30d
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM coreprod.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) between prev_91 and prev_day
                            ) active_prev_91d
                            from cte
                            order by full_date desc;""")
else:
    print("Loading data from last one month")
    active_df = spark.sql(f"""
                          
                          with cte as
                          (
                            SELECT day_date as full_date, date_add(day_date, -1) as prev_day,
                            date_add(day_date, -7) as prev_7, date_add(day_date, -30) as prev_30,
                            date_add(day_date, -91) as prev_91
                            FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar
                            where day_date > "{cutoff_dt}" and day_date <= current_date()
                            )

                            SELECT 
                            full_date 
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) = prev_day
                            ) active_prev_d
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) between prev_7 and prev_day
                            ) active_prev_7d
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) between prev_30 and prev_day
                            ) active_prev_30d
                            ,(
                                SELECT COUNT(distinct wallet_id) FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns
                                WHERE cast(event_ts as date) between prev_91 and prev_day
                            ) active_prev_91d
                            from cte
                            order by full_date desc;""")
    
active_df.createOrReplaceTempView("activity_time_series")

# COMMAND ----------

spark.sql(f"""
          MERGE INTO {catalog}.{schema}.activity_time_series AS TARGET
            USING activity_time_series AS SOURCE
            ON SOURCE.full_date = TARGET.full_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *;
            """)
