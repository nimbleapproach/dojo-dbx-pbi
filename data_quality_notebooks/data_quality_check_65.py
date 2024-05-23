# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_65

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test for job: foundations_v1_job_crm_monthly_george
# MAGIC
# MAGIC This notebook checks whether the job foundations_v1_job_crm_monthly_george has been run today.
# MAGIC ######Input widgets and apply parameters
# MAGIC

# COMMAND ----------

import datetime
from data_quality_workflow_utils import *
import time
start_time=time.time()


# COMMAND ----------

date_for_check = datetime.datetime.now(datetime.timezone.utc).date()
job_name = "foundations_v1_job_crm_monthly_george"

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
    schema = get_param('schema','')
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
# MAGIC #### Process for making API calls:

# COMMAND ----------

error_message = None

try:
    databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
    job_id = get_job_id(job_name, token, databricks_url)
    latest_run_ts = get_latest_run_ts(job_id, token, databricks_url)
    if latest_run_ts is None:
        print("Job never run")
        status = False
    elif date_for_check != latest_run_ts.date():
        print("Last run on: {0}".format(latest_run_ts.strftime("%d/%m/%Y %H:%M:%S")))
        #Not run today
        status = False
    else:
        print("Last run on: {0}".format(latest_run_ts.strftime("%d/%m/%Y %H:%M:%S")))
        status = True
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


