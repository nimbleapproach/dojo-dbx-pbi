# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_66

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test store-wise row counts of wallet_pos_txns
# MAGIC
# MAGIC This notebook checks whether the the table wallet_pos_txns has data for all stores for day-1
# MAGIC
# MAGIC ###### Input widgets and apply parameters
# MAGIC

# COMMAND ----------

import datetime
from data_quality_check_utils import *
import time
import pyspark.sql.functions as F
start_time=time.time()


# COMMAND ----------

prev_day = ((datetime.datetime.today() - datetime.timedelta(days=1))
                          .strftime('%Y-%m-%d'))

daily_tolerance = 0.5

# COMMAND ----------

def get_param(param: str, default: str = "") -> str:
    """Fetches the value of the specified parameter using dbutils.widgets.
    
    Args:
        param (str): Name of the parameter to fetch.
        default (str): Default value to return.
    Returns:
        (str): Value of the fetched parameter.
    """
    dbutils.widgets.text(param, default)
    # in case when widget returns empty string we use default value
    if (value := dbutils.widgets.get(param)) and len(value) > 0:
        return value
    else:
        return default

# COMMAND ----------

def set_global_parameters():
    """Set global configuration settings and parameters."""
    global catalog,schema, check_id, core_catalog
    catalog = get_param('catalog','')
    schema = get_param('schema', '')
    check_id = get_param('check_id','')
    core_catalog = get_param('core_catalog', '')


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Log a False result at start of test. In event of notebook failure, historic True results do not persist. 
# MAGIC
# MAGIC If the notebook runs successfully this entry will be updated at the end.

# COMMAND ----------

#get definition
set_global_parameters()

definition = spark.sql(f"""
                SELECT definition FROM 
                       {catalog}.{schema}.data_quality_check_definition
                WHERE check_id = {check_id}
                """).first()['definition']

#get job ID and run URL:
job_id = get_param('job_id', '')
azure_host = get_param('azure_host','')
url = azure_host + "/#job/" + job_id + "/run/1"

(spark.sql(f"""
          SELECT
          {check_id} as check_id,'{definition}' as definition, {False} as status, "Notebook did not complete execution." as error_message, '{url}' as run_url, null as run_duration_seconds
          """)).createOrReplaceGlobalTempView('check_results_{0}'.format(check_id))  

# COMMAND ----------

error_message = None

try:
    date_for_check = prev_day

    grouped_counts = spark.sql(f"""
        SELECT store_nbr, CAST(event_ts AS DATE) AS dt 
        FROM {core_catalog}.gb_mb_dl_tables.wallet_pos_txns 
        WHERE CAST(event_ts AS DATE) > date_add(current_date(), -10)  
        AND CAST(event_ts AS DATE) <= '{date_for_check}'
    """).alias('source')

    columns = grouped_counts.columns
    columns_to_average = []

    for element in columns:
        try:
            date = datetime.datetime.strptime(element, '%Y-%m-%d')
            if element != date_for_check:
                columns_to_average.append(element)
        except:
            pass

    median_row_count = (grouped_counts.where(f'dt != "{date_for_check}"')
                        .groupBy('store_nbr', 'dt')
                        .agg(F.count('*').alias('count'))
                        .groupBy('store_nbr')
                        .agg(F.median('count').alias('median_row_count'))).alias('med').collect()

    med_df = spark.createDataFrame(median_row_count, ['store_nbr', 'median_row_count'])

    grouped_counts = (grouped_counts.groupBy('store_nbr')
                                    .pivot('dt')
                                    .count()
                                    .join(med_df,
                                        on= med_df.store_nbr == grouped_counts.store_nbr,
                                        how='inner')
                                    .drop(med_df.store_nbr)
                                    .withColumn('low_count_cutoff', F.col('median_row_count') * daily_tolerance)
                                    .withColumn('passed_check', F.col(date_for_check) > F.col('low_count_cutoff'))
                                    ).cache()

    if grouped_counts.where(F.col('passed_check') == False).count() == 0:
        grouped_counts.show(truncate=False)
        print('passed check')
        status = True
    else:
        print('failed check:')
        (grouped_counts.where(F.col('passed_check') == False)
                        .select('store_nbr',
                                date_for_check,
                                'low_count_cutoff',
                                'passed_check').show(truncate=False))
        status = False

except Exception as e:
    status = False
    error_message = e

print("Test Result: {0}".format(status))

# COMMAND ----------

# MAGIC     
# MAGIC
# MAGIC %md
# MAGIC ######Insert result to table.

# COMMAND ----------

#Update previous entry with test results:
(spark.sql(f"""
          SELECT
          {check_id} as check_id,'{definition}' as definition, {status} as status, "{error_message}" as error_message, '{url}' as run_url, {round(time.time() - start_time, 3)} as run_duration_seconds
          """)).createOrReplaceGlobalTempView('check_results_{0}'.format(check_id))  


# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC ######Query table, get most recent results

# COMMAND ----------

# Order by timestamp
display(spark.sql(f"""SELECT *
                    FROM {catalog}.{schema}.data_quality_check_status
                    WHERE check_id == {check_id}
                    ORDER BY dts DESC
                    LIMIT 1"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Return job ID to help debugging

# COMMAND ----------

job_id = get_param('job_id', '')
dbutils.notebook.exit(job_id)


