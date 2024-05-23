# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_12

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests for game_wallets
# MAGIC
# MAGIC The purpose of this test is to check for a loss of historic data in game_wallets.
# MAGIC
# MAGIC The historic test compares the distribution of daily row counts from from now, to the distribution recorded during the last successful run of this test. The values should match.
# MAGIC
# MAGIC ######Input widgets and apply parameters
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

import datetime
from data_quality_check_utils import *
import time
start_time=time.time()

# COMMAND ----------

#define notebook params
historical_tolerance = 0.01
#Metrics we wil evaluate table history based on:
historical_metrics = ['mean_daily_row_count',
                      'max_daily_row_count',
                      'min_daily_row_count',
                      'stddev_daily_row_count',
                      'total_row_count']

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
    global catalog,schema,core_catalog, check_id
    catalog = get_param('catalog','')
    schema = get_param('schema','')
    core_catalog = get_param('core_catalog', '')
    check_id = int(get_param('check_id', ''))


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

# MAGIC %md
# MAGIC ## Run all processes
# MAGIC

# COMMAND ----------


historic_aggregation_query = """
                    WITH cte AS (SELECT *, 
                    ROW_NUMBER() OVER(PARTITION BY wallet_id, cast(event_ts as date) ORDER BY event_ts ASC) AS row_number
                    FROM {core_catalog}.gb_mb_dl_tables.game_wallets
                    WHERE CAST(event_ts AS date) <= '{{date}}')

                    SELECT CAST(event_ts AS date), 
                           COUNT(*) AS row_count FROM 
                                   cte
                    WHERE row_number = 1
                    GROUP BY CAST(event_ts AS date)
                    ORDER BY CAST(event_ts as date) desc""".format(core_catalog = core_catalog)

# COMMAND ----------

# date for check
date_for_check = ((datetime.datetime.today() - datetime.timedelta(days=1))
                          .strftime('%Y-%m-%d'))
#Run Historical test:
historical_check_result, error_message = run_historic_test(check_id = check_id,
                                                                   date_for_check = date_for_check,
                                                                   catalog = catalog,
                                                                   schema = schema,
                                                                   historical_tolerance = historical_tolerance,
                                                                   historical_query=historic_aggregation_query,
                                                                   spark = spark)
# Final Result:
status = historical_check_result

print("Final test result: {0}".format(status))

# COMMAND ----------

# MAGIC %md
# MAGIC ######Insert result to table.

# COMMAND ----------

#Update previous entry with test results:

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


