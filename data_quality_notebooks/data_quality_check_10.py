# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests for loyalty_acct
# MAGIC
# MAGIC The purpose of this test is to detect a loss of data for the current day, or duplicate data in loyalty_acct.
# MAGIC
# MAGIC It has been put in place due to irregular data loads in loyalty_acct, often lacking data for the current day. 
# MAGIC
# MAGIC The daily test compares the row counts for the current day to the median value over the previous 20 days. The row count is deemed to be 'low' if it is less than 20% of the median value over this window. 
# MAGIC
# MAGIC This test has a false positive fate of ~1%, it is therefore expected to produce false positives around 4 times per year. Two of these days are likely to be christmas and new year.
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
prev_day = 20 #number of previous days we check

daily_tolerance = 0.20
daily_metric = PercentageOfMedian()

duplication_tolerance = 0.00001
duplication_scope = 365


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
# MAGIC Historical test will only be run if the daily check passes with no error

# COMMAND ----------

daily_query = f"""
                SELECT CAST(event_ts AS date) AS dt,
                        COUNT(*) AS row_count 
                FROM {core_catalog}.gb_mb_secured_dl_tables.loyalty_acct
                WHERE CAST(event_ts AS date) > 
                    DATE_ADD(CURRENT_DATE(), -{prev_day})
                GROUP BY CAST(event_ts AS date)
                ORDER BY CAST(event_ts AS date) DESC"""

duplicate_data_query = f"""
                    SELECT CAST(event_ts AS date) AS dt,
			    COUNT(DISTINCT wallet_id) as distinct_wallet_id_count,
                            COUNT(*) AS row_count FROM 
                            {core_catalog}.gb_mb_secured_dl_tables.loyalty_acct
                WHERE CAST(event_ts AS date) > DATE_ADD(CURRENT_DATE(), -{duplication_scope})
		  	    GROUP BY CAST(event_ts AS date)
			    ORDER BY CAST(event_ts AS date)
		  		"""

# COMMAND ----------

# date for check
date_for_check = ((datetime.datetime.today() - datetime.timedelta(days=1))
                          .strftime('%Y-%m-%d'))
#Run daily row count test
daily_count_check_result, error_message = run_daily_count_test(query = daily_query,
                                                               metric = daily_metric,
                                                               date_for_check = date_for_check,
                                                               tolerance = daily_tolerance,
                                                               spark = spark)

#If previous test passes test for duplicate data:
if daily_count_check_result == True:
    duplicate_data_check_result, error_message = run_duplicate_data_test(query = duplicate_data_query,
                                                                         tolerance = duplication_tolerance,
                                                                         spark = spark)
    status = (duplicate_data_check_result & daily_count_check_result)
else:
    #Do not run more checks, status is False
    status = False

print("Final test result: {0}".format(status))

# COMMAND ----------

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


