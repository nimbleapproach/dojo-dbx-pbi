# Databricks notebook source
# MAGIC %md
# MAGIC ##Process:     GHS Participation

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

cutoff_dt = ((datetime.datetime.today() - datetime. timedelta(days=7)).strftime('%Y-%m-%d'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the row count of participation table

# COMMAND ----------

df_participation_cnt = spark.table(f"{catalog}.{schema}.ghs_participation")
tbl_cnt = df_participation_cnt.count()
display(tbl_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Condition based data load
# MAGIC #####If table has no records, then load the entire history else replace data from cutoff date

# COMMAND ----------


if (tbl_cnt == 0):
    print("Full load as the table is empty")
    dt = '2022-05-27' # Represents full load from 2022 till date
else:
    print("Data load post cut off date")
    dt = cutoff_dt # Represents load from cutoff till date

df = spark.sql(f"""
                select b.asda_wk_nbr as delivery_wk, c.asda_wk_nbr as order_wk, a.* from (
                    SELECT dlvr_dt as delivery_dt
                    , TO_DATE(order_sbmtd_ts) as order_dt
                    , outbase_store_id as store_id
                    , count(distinct web_order_id) as Total_orders
                    , count(distinct trans_rcpt_nbr) as Reward_orders
                    , count(distinct singl_profl_id) as Total_customers
                    , count(distinct wallet_id) as Reward_customers
                    , sum(case when pos_tot_amt is not null then pos_tot_amt else 0 end) as Total_Sales
                    , sum(case when trans_rcpt_nbr is not null then pos_tot_amt else 0 end) as Reward_Sales
                    ,case when  outbase_store_id in ('4372','4348','4346','4391','4378','4133','4627','4737','4711','4730','4738','4513','5878','4605','4143','4510','4252','4253','4600','4430','4586','4281','4211','4141','4837','4148','4179','4276','4327','4670','4164','4928','4275',
                    '4251','4662','4881','4564','4209','4524', '4900', '4279') then 'Wales'
                    when  outbase_store_id in ('4680','4639','4419','4934','5889','5002','4587','4955','4950','4149', '4900','4906','4610','4178','4789','4987','4988','5001','4638','4993','4692', '4279')  then 'North East' else 'Big Launch' end as Region

                    FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns pos
                    RIGHT JOIN {core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka ghs
                    ON pos.trans_rcpt_nbr=ghs.web_order_id
                    WHERE order_sbmtd_ts >= '2022-05-27'
                        AND dlvr_dt >= "{dt}"
                        AND dlvr_dt < current_date
                    GROUP BY dlvr_dt,  outbase_store_id, TO_DATE(order_sbmtd_ts)) a 
                    INNER JOIN {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar b 
                    ON a.delivery_dt = b.day_date
                    INNER JOIN {core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar c 
                    ON a.order_dt = c.day_date
                    WHERE (Region='Big Launch' AND order_dt > '2022-08-10' AND delivery_dt > '2022-08-10') OR (Region='Wales' AND order_dt > '2022-06-23' AND delivery_dt > '2022-06-23') OR
                    (Region='North East' AND order_dt > '2022-05-27' AND delivery_dt > '2022-06-23')"""
)
df.createOrReplaceTempView("participation")



# COMMAND ----------

spark.sql(f"""
          
            MERGE INTO {catalog}.{schema}.ghs_participation AS TARGET
            USING participation AS SOURCE
            ON SOURCE.delivery_dt = TARGET.delivery_dt AND
            SOURCE.order_dt = TARGET.order_dt AND
            SOURCE.store_id = TARGET.store_id
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *;
             
            """)
