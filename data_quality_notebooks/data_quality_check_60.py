# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_60

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test datamart table dsa_pbi_ar_sdb_date_hierarchy
# MAGIC
# MAGIC This notebook checks whether the the table dsa_pbi_ar_sdb_date_hierarchy has data for the current day -1
# MAGIC
# MAGIC ###### Input widgets and apply parameters
# MAGIC

# COMMAND ----------

import datetime
from data_quality_check_utils import *
import time
start_time=time.time()


# COMMAND ----------

prev_day = ((datetime.datetime.today() - datetime.timedelta(days=1))
                          .strftime('%Y-%m-%d'))
day_minus_2 = ((datetime.datetime.today() - datetime.timedelta(days=2))
                          .strftime('%Y-%m-%d'))

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
# MAGIC ### Define date column query
# MAGIC
# MAGIC Query should produce a column called 'dt' to be tested

# COMMAND ----------

set_global_parameters()
date_column_query = f"""
                    SELECT CAST(visit_date AS date) AS dt FROM {catalog}.cdna_ar.dsa_pbi_ar_sdb_date_hierarchy
                    GROUP BY CAST(visit_date AS date)
                    ORDER BY CAST(visit_date AS date) DESC
                    limit 20
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Process for Testing recent date in table:

# COMMAND ----------

error_message = None

try:
    status = date_column_check(date_column_query, prev_day, spark) | date_column_check(date_column_query, day_minus_2, spark)
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


