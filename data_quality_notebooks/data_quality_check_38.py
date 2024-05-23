# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_38

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests for cdd_odl_dim_item_hierarchy
# MAGIC
# MAGIC The purpose of this test is to detect a loss of data for the current day
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
# MAGIC ### Define Daily Data Load and Test functions
# MAGIC ###### Get daily counts according to global parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run all processes
# MAGIC
# MAGIC Historical test will only be run if the daily check passes with no error

# COMMAND ----------

#init global parameters
error_message = None
daily_query = f"""SELECT CAST(md_created_ts as date) as load_dt, COUNT(*) 
                FROM {core_catalog}.gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy 
                GROUP BY CAST(md_created_ts as date) 
                ORDER BY CAST(md_created_ts as date) desc"""

# COMMAND ----------

# date for check
date_for_check = ((datetime.datetime.today() - datetime.timedelta(days=0)))
try:
    load_date = spark.sql(daily_query).select('load_dt').collect()
    print('Load date: {0}'.format(load_date[0][0]))
    status = load_date[0][0] == date_for_check.date()
except Exception as e:
    error_message = e
    status = False


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


