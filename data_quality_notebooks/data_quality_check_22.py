# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_check_22

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test for app_usage_api 
# MAGIC
# MAGIC The purpose of this test is to detect a loss of data for the current day in bi_data_model.app_usage_api.
# MAGIC
# MAGIC This table is appended to daily by a databricks workflow.
# MAGIC
# MAGIC It uses the the median value to test for a value in 'active_users' for today. If todays value is less than 30% of the median of the values for the previous 20 days, then today's value is classed as an outlier.
# MAGIC
# MAGIC This metric was selected through historical examination of the data.
# MAGIC
# MAGIC ######Input widgets and apply parameters
# MAGIC 

# COMMAND ----------
import datetime
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import Optional, Tuple
from pyspark.sql.types import StructType, StructField, BooleanType, FloatType, StringType, DateType
import time
start_time=time.time()

# COMMAND ----------
#define notebook params
prev_day = 20 #number of previous days we check
daily_tolerance = 0.30

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
# MAGIC ### Define Daily Data Load and Test functions
# MAGIC ###### Get daily counts according to global parameters

# COMMAND ----------
def load_daily_counts(previous_days: int) -> Tuple[Optional[DataFrame], Optional[str]]:
    """
    Load the daily counts from active_earners. 
    Args:
        previous_days (int): The number of days we wish to examine
    Returns:
        daily_count_df Optional(bool): The daily row counts
        err_msg Optional(str): Contains any error in query.
    
    """
    try:
        #get counts over specified window
        daily_count_df = spark.sql(f"""
                            SELECT date as dt,
                            activeUsers as active_users
                            FROM {catalog}.{schema}.app_usage_api
                            """)
        
        daily_count_df = (daily_count_df.withColumn("full_date", 
                                                    F.to_date(daily_count_df["dt"], "yyyyMMdd").cast(DateType()))
                                        .filter(f"full_date > DATE_ADD(CURRENT_DATE(), - {previous_days}) "))
        
        daily_count_df = daily_count_df.orderBy(daily_count_df.full_date.desc())
        
        return daily_count_df, None
    except Exception as e:
        err_msg = e
        return None, str(err_msg)

# COMMAND ----------
def test_daily_counts(daily_count_df: DataFrame, tolerance: float, date_for_check: str) -> bool:
    """
    Test the daily count value against the designated cutoff

    Args:
        daily_count_df (DataFrame): The daily row counts for the selected table
        tolerance (float): The value to scale the count cutoff
    Returns:
        daily_check_result (bool): True if check passes, False otherwise
    """
    #define low counts based on median
    daily_count_df = (daily_count_df
                            .withColumn("low_count_cuttoff",
                            F.lit(daily_count_df
                                .select(F.median('active_users'))
                                .collect()[0]['median(active_users)'] * tolerance)))

    #Flag low counts as less than cutoff defined above
    daily_count_df = (daily_count_df
                            .withColumn("pass_check",
                                F.when((daily_count_df.active_users < 
                                        daily_count_df.low_count_cuttoff), False)
                                        .otherwise(True)))
    #Display results
    display(daily_count_df.cache())
    #Return test result for most recent date. Return False if no record for today.
    if daily_count_df.filter(daily_count_df.full_date == date_for_check).count() == 0:
        daily_count_check_result = False
    else:
       daily_count_check_result = (daily_count_df
                                .filter(daily_count_df.full_date == date_for_check)
                                .select('pass_check').collect()[0][0]) 
    return daily_count_check_result

# COMMAND ----------
def run_daily_count_test(prev_day:int , date_for_check: str, tolerance: float) -> Tuple[bool, Optional[str]]:
    """
    Function which executes all the steps required to run a test against the daily counts

    Args:
        prev_day (int): The number of previous days the test should consider
        tolerance (float): The value to scale the cutoff of the test.
    Returns:
        daily_count_check_result: True if test has passed, False otherwise
        error_message (Optional[str]): Any error associated with the query
    """
    daily_count_df, error_message = load_daily_counts(previous_days=prev_day)
    if error_message is None and daily_count_df is not None:
        #run daily row count test
        daily_count_check_result = test_daily_counts(daily_count_df = daily_count_df, tolerance=tolerance, date_for_check = date_for_check)
        print("Daily count test result: {0}".format(daily_count_check_result))
    else:
        #Handle error:
        print("ERROR in daily count data load:")
        print(error_message)
        daily_count_check_result = False
    
    return daily_count_check_result, error_message


# COMMAND ----------
# MAGIC %md
# MAGIC ## Run all processes
# MAGIC 
# MAGIC Historical test will only be run if the daily check passes with no error

# COMMAND ----------
# date for check
date_for_check = ((datetime.datetime.today() - datetime.timedelta(days=1))
                          .strftime('%Y-%m-%d'))
#Run daily row count test
daily_count_check_result, error_message = run_daily_count_test(prev_day, date_for_check, daily_tolerance)
status = daily_count_check_result

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


