# Databricks notebook source
# MAGIC %md
# MAGIC ##Process:     active earners

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

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the count of active earners table

# COMMAND ----------

df_active_earners_cnt = spark.table(f"{catalog}.{schema}.active_earners")
tbl_cnt = df_active_earners_cnt.count()
display(tbl_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the count of missing dates in active earners table

# COMMAND ----------

missing_days = spark.sql(f"""
                            select distinct day_date from coreprod.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar a
                            left join {catalog}.{schema}.active_earners b
                            on a.day_date = b.full_date
                            where b.full_date is null
                            /* 2021-09-08 is the minimum date for which the data is currently present in the table */
                            and a.day_date>='2021-09-08' and a.day_date <= "{cutoff_dt}"; 
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
               WITH cte1 as (

                            SELECT 
                            dt.full_date,
                            COUNT(distinct mw.wallet_id) earning_7d

                            FROM (
                                    SELECT 
                                    dateadd(cd.day_date, -1) as full_date
                                    FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar cd
                                    where cd.day_date <= current_date()
                                )  dt

                            JOIN (

                                    SELECT 
                                    DISTINCT mw.wallet_id,
                                    mw.cmpgn_cmplt_ts dt
                                    FROM {core_catalog}.gb_mb_dl_tables.mssn_wallets mw
                                    LEFT JOIN {core_catalog}.gb_mb_dl_tables.wallet_pos_txns wpt
                                    on mw.wallet_id = wpt.wallet_id
                                    and mw.cmpgn_cmplt_ts = wpt.event_ts

                                    WHERE cast(mw.cmpgn_cmplt_ts as date) < CURRENT_DATE

                                    and cast(mw.cmpgn_cmplt_ts as date) >= cast('2021-09-05' as date)
                                    and mw.cmpgn_cmplt_ts is not NULL
                                    and mw.rec_status_ind='CURRENT'
                                    and mw.cmpgn_id <> 522467

                                    UNION

                                    SELECT 
                                    DISTINCT gw.wallet_id,
                                    game_redm_ts dt
                                    FROM {core_catalog}.gb_mb_dl_tables.game_wallets gw
                                    WHERE cast(gw.game_redm_ts as date) < CURRENT_DATE
                                    and gw.game_redm_ts is not null
                                    and gw.rec_status_ind like 'CURRENT'


                                )mw

                                ON cast(mw.dt as date) <= dt.full_date
                                WHERE cast(mw.dt as date) between date_add(dt.full_date, -6) and date_add(dt.full_date, 0)
                                GROUP BY dt.full_date

                            )

                ,cte2 as (

                            SELECT 
                            dt.full_date, 
                            COUNT(distinct wpt.wallet_id) active_7d

                            FROM (

                                    SELECT 
                                    dateadd(cd.day_date, -1) as full_date
                                    FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar cd
                                    where cd.day_date <= current_date()

                                ) dt

                                JOIN
                                (

                                    SELECT *
                                    FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns w
                                    WHERE cast(w.event_ts as date) < CURRENT_DATE
                                    and cast(w.event_ts as date) >= cast('2021-09-05' as date)


                                ) wpt

                                ON cast(wpt.event_ts as date) <= dt.full_date
                                WHERE cast(wpt.event_ts as date) between date_add(dt.full_date, -6) and date_add(dt.full_date, 0)
                                GROUP BY dt.full_date

                        )
                SELECT cte1.full_date, earning_7d, active_7d, cast(earning_7d as float) / cast(active_7d as float) as percentage
                FROM cte1
                JOIN cte2
                on cte1.full_date = cte2.full_date;
                """)
    
else:
    print("Loading data from last one month")
    active_df = spark.sql(f"""
                          
                    WITH cte1 as (

                            SELECT 
                            dt.full_date,
                            COUNT(distinct mw.wallet_id) earning_7d

                            FROM (
                                    SELECT 
                                    dateadd(cd.day_date, -1) as full_date
                                    FROM {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar cd
                                    where cd.day_date <= current_date()
                                    and cd.day_date > "{cutoff_dt}"
                                )  dt

                            JOIN (

                                    SELECT 
                                    DISTINCT mw.wallet_id,
                                    mw.cmpgn_cmplt_ts dt
                                    FROM {core_catalog}.gb_mb_dl_tables.mssn_wallets mw
                                    LEFT JOIN coreprod.gb_mb_dl_tables.wallet_pos_txns wpt
                                    on mw.wallet_id = wpt.wallet_id
                                    and mw.cmpgn_cmplt_ts = wpt.event_ts

                                    WHERE cast(mw.cmpgn_cmplt_ts as date) < CURRENT_DATE

                                    and cast(mw.cmpgn_cmplt_ts as date) >= cast('2021-09-05' as date)
                                    and mw.cmpgn_cmplt_ts is not NULL
                                    and mw.rec_status_ind='CURRENT'
                                    and mw.cmpgn_id <> 522467

                                    UNION

                                    SELECT 
                                    DISTINCT gw.wallet_id,
                                    game_redm_ts dt
                                    FROM {core_catalog}.gb_mb_dl_tables.game_wallets gw
                                    WHERE cast(gw.game_redm_ts as date) < CURRENT_DATE
                                    and gw.game_redm_ts is not null
                                    and gw.rec_status_ind like 'CURRENT'


                                )mw

                                ON cast(mw.dt as date) <= dt.full_date
                                WHERE cast(mw.dt as date) between date_add(dt.full_date, -6) and date_add(dt.full_date, 0)
                                GROUP BY dt.full_date

                            )

                ,cte2 as (

                            SELECT 
                            dt.full_date, 
                            COUNT(distinct wpt.wallet_id) active_7d

                            FROM (

                                    SELECT 
                                    dateadd(cd.day_date, -1) as full_date
                                    FROM 
                                    {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar cd
                                    where cd.day_date <= current_date()
                                    and cd.day_date > "{cutoff_dt}"

                                ) dt

                                JOIN
                                (

                                    SELECT *
                                    FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns w
                                    WHERE cast(w.event_ts as date) < CURRENT_DATE
                                    and cast(w.event_ts as date) >= cast('2021-09-05' as date)


                                ) wpt

                                ON cast(wpt.event_ts as date) <= dt.full_date
                                WHERE cast(wpt.event_ts as date) between date_add(dt.full_date, -6) and date_add(dt.full_date, 0)
                                GROUP BY dt.full_date

                        )
                SELECT cte1.full_date, earning_7d, active_7d, cast(earning_7d as float) / cast(active_7d as float) as percentage
                FROM cte1
                JOIN cte2
                on cte1.full_date = cte2.full_date;
                """)
    
active_df.createOrReplaceTempView("active_earners") 
    
    

# COMMAND ----------

spark.sql(f"""
          
            MERGE INTO {catalog}.{schema}.active_earners AS TARGET
            USING active_earners AS SOURCE
            ON SOURCE.full_date = TARGET.full_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *;
             
            """)
