# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_55

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test datamart table dsa_pbi_ar_gsm_wallets
# MAGIC
# MAGIC This notebook checks whether the most recent data in table dsa_pbi_ar_gsm_wallets is from last complete week
# MAGIC ###### Input widgets and apply parameters
# MAGIC

# COMMAND ----------

import datetime
from data_quality_check_utils import *
import time
from pyspark.sql import functions as F
start_time=time.time()


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
    global catalog,schema, check_id
    catalog = get_param('catalog','')
    schema = get_param('schema', '')
    check_id = get_param('check_id','')


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
# MAGIC
# MAGIC ### Get current and last week

# COMMAND ----------

get_last_week_number_query = f"""
                    SELECT week as week_nbr, week_start_day, week_end_day FROM {catalog}.cdna_ar.dsa_helper_week_finder
                    WHERE week_start_day < DATE_ADD(CURRENT_DATE(), -7) AND
                          week_end_day > DATE_ADD(CURRENT_DATE(), -7)
"""

table_query = f"""
                SELECT week as week_nbr FROM {catalog}.cdna_ar.dsa_pbi_ar_gsm_wallets
                    GROUP BY week
                    ORDER BY week DESC
                    limit 20
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Process for Testing recent date in table:

# COMMAND ----------

error_message = None

try:
    last_week_nbr = spark.sql(get_last_week_number_query).cache()
    if last_week_nbr.count() != 1:
        raise Exception('Could not find last complete week number in dsa_helper_week_finder')
    display(last_week_nbr)
    # Get last week number from helper table
    last_week_nbr = last_week_nbr.select('week_nbr').collect()[0][0]
    # Now check the table to be tested:
    most_recent_week_in_table = (spark.sql(table_query).cache())
    display(most_recent_week_in_table)
    most_recent_week_in_table = most_recent_week_in_table.agg(F.max('week_nbr')).collect()[0][0]

    if most_recent_week_in_table == last_week_nbr:
        status = True
    else:
        status = False

    print(f'most recent week nbr in table: {most_recent_week_in_table}')
    print(f'last complete week nbr: {last_week_nbr}')

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


