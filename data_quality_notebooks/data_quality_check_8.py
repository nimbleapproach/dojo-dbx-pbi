# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_8

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test against average value for giveaway percentage
# MAGIC ######Input widgets and apply parameters
# MAGIC

# COMMAND ----------

import datetime
import pyspark.sql.functions as F
from typing import Optional, Tuple
import time
start_time=time.time()
# Track time for performance profiling


# COMMAND ----------

#define notebook params
prev_day = 10 #we discount todays counts, so checking previous 9 days
tolerance = 0.85

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
# MAGIC ###Data Load and Test
# MAGIC ###### Get daily counts according to global parameters

# COMMAND ----------

def daily_count_check(previous_days: int, tolerance: float) -> Tuple[bool, Optional[str]]:
    """
    Load and test the giveaway percentage at visit level

    Args:
        previous_days (int): The number of days we wish to examine
        tolerance (float): The tolerance between adjacent days
    Returns:
        daily_count_check_result (bool): True if row count meets critera,
                                         False otherwise.
        err_msg Optional(str): Contains any error in query.
    
    """
    err_msg=None
    try:
        #date for check:
        date_for_check = ((datetime.datetime.today() - datetime.timedelta(days=1))
                          .strftime('%Y-%m-%d'))
        #get counts over specified window
        daily_count_df = spark.sql(f"""SELECT
	                CAST(visit_dt AS date) as dt,
	                SUM(earn)*100/SUM(ar_sales) AS giveaway
                FROM {catalog}.{schema}.eof_visit_level 
                WHERE store_nbr = 4582 
                AND CAST(visit_dt AS date) > (DATE_ADD(CURRENT_DATE(), -{prev_day}))
                GROUP BY CAST(visit_dt AS date)
                ORDER BY CAST(visit_dt AS date) DESC""").cache()
        #Define 'low counts' according to tolerance and median value
        daily_count_df = (daily_count_df
                                .withColumn("low_count_cuttoff",
                                F.lit(daily_count_df
                                .filter(daily_count_df.dt < date_for_check)
                                .select(F.median('giveaway')) #Low count cuttoff defined by a percentage of the median
                                .collect()[0]['median(giveaway)'] * tolerance)))
        #Run logical checks on daily count
        daily_count_df = (daily_count_df
                          .withColumn("pass_check",
                                F.when(((daily_count_df.giveaway < 
                                        daily_count_df.low_count_cuttoff) | (daily_count_df.giveaway.isNull())) , False)
                                        .otherwise(True)))
        #Display results
        display(daily_count_df.cache())
        
        #Return test result for most recent date. Return False if no record for today.
        if daily_count_df.filter(daily_count_df.dt == date_for_check).count() == 0:
            daily_count_check_result = False
        else:
            daily_count_check_result = (daily_count_df
                                .filter(daily_count_df.dt == date_for_check)
                                .first()['pass_check'])

        return daily_count_check_result, err_msg
    except Exception as e:
        err_msg = e
        return False, str(err_msg)
    


# COMMAND ----------

#init methods and run test
status, error_message = daily_count_check(previous_days=prev_day,tolerance=tolerance)

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
