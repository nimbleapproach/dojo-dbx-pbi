# Databricks notebook source
# MAGIC %md
# MAGIC ##Process:     opt_in_figures_airship
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating widgets
# MAGIC ######Input widgets and apply parameters 

# COMMAND ----------

dbutils.widgets.text('core_catalog', 'coreprod')
dbutils.widgets.text('catalog', '')
dbutils.widgets.text('schema', '')


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

# MAGIC %md
# MAGIC ###Transformation
# MAGIC ######1: Query coreprod data into a temporay view  2: Import csv from blob storage  3: Merge both data sets together 

# COMMAND ----------

import pandas as pd
import numpy as np
import datetime as dt
import smtplib
import datetime as dt
import csv
import os
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType,BooleanType,DateType
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting the Loyalty data
# MAGIC
# MAGIC Note that here the push_opt_in is the initial value

# COMMAND ----------

df_loyalty = spark.sql(f"""
select 
        a.singl_profl_id as singl_profl_id
        ,a.acct_status_id as acct_status_id
        ,a.regtn_ts as regtn_ts
                ,a.guest_ind as guest_ind
        ,case when (s.registration_channel = 'LOYALTY' or cast(s.registration_date as date) = cast(s.tnc_accepted_at_loyalty as date) )THEN 1 else 0 END AS new_ind
        ,case when p.is_email_contactable ='Y' then 1 else 0 end as mktg_opt_in
        ,case when c.push_notif_consent  ='Y' then 1 else 0 end as push_opt_in
        ,case when (regtn_ts<date_add(current_timestamp, -37) and a.acct_status_id='TEMP') then 1 else 0
        end as acct_del_ind_1
        from
    ( 
        select DISTINCT case when singl_profl_id is not null then singl_profl_id
            when singl_profl_id is null then concat('no_assgnd_spid_', cast   (la.wallet_id as varchar(50)))
            end as singl_profl_id
            ,regtn_ts
            ,acct_status_id
                        ,case when regtn_ts <> dt_cnvrt_to_full_acct_ts then 1
        when dt_cnvrt_to_full_acct_ts is null  then 1 else 0 
    end as guest_ind
    from {core_catalog}.gb_mb_secured_dl_tables.loyalty_acct la
        where 1=1
        and cast(regtn_ts as date) < CURRENT_DATE
    ) a
    LEFT OUTER JOIN {core_catalog}.gb_customer_data_domain_secured_rpt.cdd_rpt_loyalty_acct c
    ON a.singl_profl_id = c.singl_profl_id
    LEFT OUTER JOIN {core_catalog}.gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer s
    ON a.singl_profl_id = s.singl_profl_id
    LEFT OUTER JOIN {core_catalog}.gb_customer_data_domain_secured_rpt.cdd_rpt_ft_pc_global_contactable_email p
    ON a.singl_profl_id = p.single_profile_id
""")

# COMMAND ----------

df_loyalty = df_loyalty.withColumnRenamed('push_opt_in', 'push_opt_in_loyalty')

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting the airship data in
# MAGIC
# MAGIC This will then be joined onto the loyalty data to get the new push_opt_in

# COMMAND ----------


#Method using Volume:

airship_files = spark.sql(f'''LIST "/Volumes/{catalog}/{schema}/ext_vol_{schema}/airship"''')
#Select most recent file that was added:
file_name = (airship_files.orderBy(F.desc('modification_time'))
                          .limit(1)
                          .select('name')
                          .collect()[0][0])

if '.csv' in file_name:
    read_format = 'csv'
elif '.parquet' in file_name:
    read_format = 'parquet'
else:
    raise Exception('File format unknown: {0}'.format(file_name))



# COMMAND ----------

#Mehtod Using Volume:
  
df_airship_opt =  (spark.read
  .format(read_format)
  .options(header='True')
  .load(f"/Volumes/{catalog}/{schema}/ext_vol_{schema}/airship/{file_name}")
).cache()

display(df_airship_opt)

# COMMAND ----------

df_airship_opt= df_airship_opt.withColumnRenamed('Tags Current Notification Opt-in','Notification Opt-in')

# COMMAND ----------

df_airship_opt = df_airship_opt.na.drop(subset=["Notification Opt-in"])

# COMMAND ----------

df_airship_opt = df_airship_opt.withColumn('Notification Opt-in', when(col('Notification Opt-in') == 'true', 1).otherwise(0))

# COMMAND ----------

df_airship_opt = df_airship_opt.withColumnRenamed('User IDs Named User', 'Named User')

# COMMAND ----------

df_airship_opt = df_airship_opt.select('Named User', 'Notification Opt-in')

# COMMAND ----------

df_airship_opt= df_airship_opt.withColumnRenamed('Named User', 'SPID').withColumnRenamed('Notification Opt-in','push_opt_in_airship')

# COMMAND ----------

df_airship_opt = df_airship_opt.groupBy('SPID').max("push_opt_in_airship").withColumnRenamed('max(push_opt_in_airship)', 'push_opt_in_airship')

# COMMAND ----------

# MAGIC %md
# MAGIC # Left join loyalty and airship data
# MAGIC
# MAGIC Then we will update the push_opt_in where it exists in airship, otherwise stick to what loyalty says

# COMMAND ----------

#merged = pd.merge(df_loyalty, df_airship_opt, left_on = 'singl_profl_id', right_on = 'SPID', how = 'left')

merged = df_loyalty.join(df_airship_opt, df_loyalty.singl_profl_id == df_airship_opt.SPID,'left')

# COMMAND ----------

merged = merged.withColumn('push_opt_in', col('push_opt_in_airship'))

# COMMAND ----------

all_SPIDS = merged.where(merged.acct_status_id == "DEFAULT").select('singl_profl_id').count()
all_either_opt_in = merged.where((merged.acct_status_id == "DEFAULT") & ((merged.mktg_opt_in == 1) | (merged.push_opt_in == 1))).select('singl_profl_id').count()
all_email_opt_in = merged.where((merged.acct_status_id == "DEFAULT") & (merged.mktg_opt_in == 1)).select('singl_profl_id').count()
all_push_opt_in = merged.where((merged.acct_status_id == "DEFAULT") & ( merged.push_opt_in == 1)).select('singl_profl_id').count()

# COMMAND ----------

all_opt_perc = (all_either_opt_in/all_SPIDS)
email_opt_perc = (all_email_opt_in/all_SPIDS)
push_opt_perc = (all_push_opt_in/all_SPIDS)

print(f"Overall opt in rate: {all_opt_perc}")
print(f"Email opt in rate: {email_opt_perc}")
print(f"Push opt in rate: {push_opt_perc}")

# COMMAND ----------

all_spids_list = ['all spids', all_SPIDS, all_either_opt_in, all_opt_perc, all_email_opt_in, email_opt_perc, all_push_opt_in, push_opt_perc]

# COMMAND ----------

new_SPIDS = merged.where((merged.new_ind == 1) & (merged.acct_status_id == "DEFAULT")).select('singl_profl_id').count()
new_either_opt_in = merged.where((merged.new_ind == 1) & (merged.acct_status_id == "DEFAULT") & ((merged.mktg_opt_in == 1) | (merged.push_opt_in == 1))).select('singl_profl_id').count()
new_email_opt_in = merged.where((merged.new_ind == 1) & (merged.acct_status_id == "DEFAULT") & (merged.mktg_opt_in == 1)).select('singl_profl_id').count()
new_push_opt_in = merged.where((merged.new_ind == 1) & (merged.acct_status_id == "DEFAULT") & ( merged.push_opt_in == 1)).select('singl_profl_id').count()

# COMMAND ----------

new_opt_perc = (new_either_opt_in/new_SPIDS)
new_email_opt_perc = (new_email_opt_in/new_SPIDS)
new_push_opt_perc = (new_push_opt_in/new_SPIDS)



print(f"New Overall opt in rate: {new_opt_perc}")
print(f"New Email opt in rate: {new_email_opt_perc}")
print(f"New Push opt in rate: {new_push_opt_perc}")

# COMMAND ----------

new_spids_list = ['new spids', new_SPIDS, new_either_opt_in, new_opt_perc, new_email_opt_in, new_email_opt_perc, new_push_opt_in, new_push_opt_perc]

# COMMAND ----------

results = spark.createDataFrame([all_spids_list, new_spids_list]).toDF('type','Total_spids', 'num_opt_in','perc_opt_in','num_email_opt_in', 'perc_email_opt_in', 'num_push_opt_in', 'perc_push_opt_in')

# COMMAND ----------

results.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save to Table
# MAGIC
# MAGIC Maybe append to a CSV so we have historic data also?

# COMMAND ----------


results.createOrReplaceTempView("opt_in_inc_airship")
 
spark.sql(f"""INSERT OVERWRITE {catalog}.{schema}.opt_in_inc_airship TABLE opt_in_inc_airship;""")

