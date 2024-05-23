# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Quality Presentation Layer
# MAGIC
# MAGIC Calculate metrics about our workflows and checks to display in the report.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Import modules and define functions:

# COMMAND ----------

import pyspark.sql.functions as F
import datetime
from typing import *
from pyspark.sql.types import StringType, BooleanType, StructType, StructField, LongType
from pyspark.sql import DataFrame

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
    global catalog,schema
    catalog = get_param('catalog','')
    schema = get_param('schema','')


# COMMAND ----------

def get_ts_from_microseconds(ts: Optional[str]) -> Optional[str]:
    """
    Extracts the timestamp from the latest job run

    Args:
        ts: timestamp to convert, Time stamp should be in microseconds from 01/01/1970
    Returns:
       formatted_ts: Timestamp in a date-string format
    
    """
    if ts is None:
        return None
    elif int(ts) == 0:
        return None
    else:

        ts = int(ts)
        reference_time = datetime.datetime(1970,1,1, tzinfo=datetime.timezone.utc)
        time_difference = datetime.timedelta(microseconds=ts*1000)
        formatted_ts = reference_time + time_difference
        formatted_ts = formatted_ts.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_ts


# COMMAND ----------

def conditionally_parse_databricks_state(state: str):
    """
    Function returns the result of a databricks workflow from a json

    Args: 
        state (str): Json in string form containing run state
    Returns:
        str: The result of run in a standardised string format.


    
    """
    if state is None:
        #This is a common error source - if workflow has not been run then state is None
        return 'Never Run'
    state = state.replace('false', "False").replace('true', 'True').replace('null', 'None')
    state = eval(state)
    if state['life_cycle_state'] == 'TERMINATED':
        if state['result_state'] == 'SUCCESS':
            return 'Run Completed'
        else:
            return 'Run Failed'
    else:
        return  'Running'

# COMMAND ----------

def conditionally_format_pbi_state(state):
    """Reformats the string 'Unknown' to 'Running'"""
    if state == 'Unknown':
        return 'Refreshing'
    if state == 'Completed':
        return 'Refresh Successful'
    if state == 'Failed':
        return 'Refresh Failed'
    else:
        return state
    

# COMMAND ----------

def get_dependency_parameter(dependency_id: int, parameter_name: str = 'schedule type'):
    """
    Gets the selected parameter associated with the dependency

    Args:
        dependency_id (int): The dependency ID to get the parameter for
        parameter_name (str): The parameter we want
    Returns:
        (str): The parameter value
    Raises:
        Exception: If there are multiple parameter columns for one dependency
        Exception: If there are no parameter value for the dependency
        Exception: If the parameter is not passed in a dictionary format
        Exception: If the paramter selected is not in the parameter dictionary
    """
    dependency_id = int(dependency_id)
    parameters_df = spark.sql(f"""
          SELECT parameter FROM {catalog}.{schema}.data_quality_dependency_definition
          WHERE dependency_id = {dependency_id}
          """).cache()
    
    #Check that we have just 1 paramter column
    if parameters_df.count() > 1:

        raise Exception("More than one set of parameters found for dependency ID {0}".format(dependency_id))
    if parameters_df.count() == 0:

        raise Exception("No parameters found for dependency ID {0}".format(dependency_id))
    
    #Read the paramter column value as a dictionary
    try:
        parameters = eval(parameters_df.collect()[0][0])
    except Exception as e:
        raise Exception('Error in trying to parse parameters to dictionary.')
    #If the parameter selected is in the dictionry, return it:
    if parameter_name not in parameters:

        raise Exception("Parameter '{0}' not in parameters for dependecy id {1}.".format(parameter_name, dependency_id))
    else:

        return parameters[parameter_name]
    

# COMMAND ----------

def get_status_flag(status: str, schedule: str, run_start_ts: str) -> bool:
    """
    Checks wether the latest run was within the schedule window. 
    For example, if the process has a daily schedule, but has not been run today, it will return False

    Args:
        status (str): The current status of the run
        schedule (str): The schedule of the process
        run_start_ts (str): The start time of the last run

    Returns:
        (bool): True if the run was within the schedule window, False otherwise

    Raises:
        Exception: Error if the schedule type is not one of the defined values.
    """
    if run_start_ts is None:
        #The workflow has not run before
        return False
    ts = datetime.datetime.strptime(run_start_ts, '%Y-%m-%d %H:%M:%S')
    if status == 'Refresh Successful' or status == 'Run Completed':
        if schedule == 'Daily':
            if (ts.date() == datetime.datetime.now(datetime.timezone.utc).date()):
                return True
            else:
                return False
        elif schedule == 'Weekly':
            if (datetime.datetime.now(datetime.timezone.utc).date() - ts.date()).days < 7:
                return True
            else:
                return False
        elif schedule == 'Twice Daily':

            current_time = datetime.datetime.now(datetime.timezone.utc)
            before_midday = current_time.hour <= 12
            if ((before_midday and ts.hour<=12) or (before_midday==False and ts.hour>12)):
                return True
            else:
                return False

        elif schedule == '':
            return False
        else:
            raise Exception('Unexpected Schedule.')
    else:
        return False

#UDF of the above function to allow it to be applied column-wise
udf_get_status = F.udf(get_status_flag, BooleanType())

# COMMAND ----------

def parse_power_bi_json(dependency_info: DataFrame) -> DataFrame:
    """
    Parse info from the json in the table data_quality_dependency_info, specifically for PBI reports
    
    Args:
        dependency_info (DataFrame): All info for dependencies
    
    Returns:
        power_bi_info_parsed (DataFrame): Dataframe with columns containing info from the Json
    """

    # Seperate into databricks and PBI Dependencies as both contain different info
    power_bi_info = dependency_info.filter("type = 'Power BI Report'").cache()

    # Parse Power BI Json
    power_bi_info_parsed = power_bi_info.select('dependency_id', 'dts', 'type', 'name', F.json_tuple(F.col("info"),
                                        "refresh_id", "refresh_status", 'refresh_start_time', 'refresh_end_time', 'error_message', 'refresh_duration_in_seconds')
                    ).toDF('dependency_id', 'dts', 'type', 'name', "workflow_id", "refresh_status", 'start_time', 'end_time', 'error_message', 'duration_in_seconds').cache()
    # Get the current refresh status:
    udf_format_pbi_state = F.udf(conditionally_format_pbi_state, StringType())
    power_bi_info_parsed = power_bi_info_parsed.withColumn('refresh_status', udf_format_pbi_state("refresh_status")).cache()  
    return power_bi_info_parsed

# COMMAND ----------

def parse_databricks_json(dependency_info: DataFrame) -> DataFrame:
    """
    Parse info from the json in the table data_quality_dependency_info, specifically for databricks workfloww
    
    Args:
        dependency_info (DataFrame): All info for dependencies
    
    Returns:
        power_bi_info_parsed (DataFrame): Dataframe with columns containing info from the Json
    """
    # Parse databricks Json
    databricks_info = dependency_info.filter("type = 'Databricks Workflow'").cache()

    # Import that the order of columns is the same as above due to union join
    databricks_info_parsed = databricks_info.select('dependency_id', 'dts', 'type', 'name', F.json_tuple(F.col("info"),
                                        "run_id", "state", 'start_time', 'end_time', 'error_message')
                    ).toDF('dependency_id', 'dts', 'type', 'name', "workflow_id", "refresh_status", 'start_time', 'end_time', 'error_message').cache()
    udf_get_ts_from_microseconds = F.udf(get_ts_from_microseconds, StringType())
    udf_parse_databricks_state = F.udf(conditionally_parse_databricks_state, StringType())
    databricks_info_parsed = databricks_info_parsed.withColumn("duration_in_seconds", F.when(databricks_info_parsed.end_time == 0,F.lit(None)).otherwise((F.col('end_time') - F.col('start_time'))/1000)) 
    databricks_info_parsed = (databricks_info_parsed.withColumn("start_time", udf_get_ts_from_microseconds("start_time"))
                                                    .withColumn("end_time", udf_get_ts_from_microseconds("end_time"))).cache()
    databricks_info_parsed = databricks_info_parsed.withColumn('refresh_status', udf_parse_databricks_state("refresh_status")).cache()   
    return databricks_info_parsed

# COMMAND ----------

def create_dependency_presentation_layer(databricks_info_parsed: DataFrame, power_bi_info_parsed: DataFrame) -> None:
    """
    Create a dataframe for presentation in PBI Report, showing parsed info from the logging dataframe.
    Result of this function is a temp view called 'source', to merge into the existing presentation layer table.

    Args:
        databricks_info_parsed (DataFrame): The parsed data from the logging table, for Databricks workflows
        power_bi_info_parsed (DataFrame): The parsed data from the logging table, for PBI workflows
    """
    # Show the dependency Information Layer:
    dependendency_presentation_layer = databricks_info_parsed.union(power_bi_info_parsed).orderBy('dependency_id').cache()
    #Only select most recent entries:
    dependency_ids = dependendency_presentation_layer.select('dependency_id').distinct()

    #Add schedule information:
    d_id_schema = StructType([StructField('d_id', LongType(), True), 
                            StructField('schedule', StringType(), True)])
    # create dataframe of dependency ID and schedule Type:
    dependency_ids = [(x[0], get_dependency_parameter(x[0])) for x in dependency_ids.collect()]
    dependency_ids = spark.createDataFrame(dependency_ids, d_id_schema)
    #Join on dependency ID to include schedule information
    dependendency_presentation_layer = dependendency_presentation_layer.join(dependency_ids, dependency_ids.d_id == dependendency_presentation_layer.dependency_id).drop('d_id')

    # Include run_status_flag -> a column which checks whether the last refresh was consistent with the goal schedule
    dependendency_presentation_layer = dependendency_presentation_layer.withColumn('run_status_flag', udf_get_status('refresh_status', 'schedule', 'start_time')).cache()
    dependendency_presentation_layer = dependendency_presentation_layer.withColumnRenamed('refresh_status', 'status')
    
    display(dependendency_presentation_layer)
    dependendency_presentation_layer.createOrReplaceTempView('source')
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC #### Functions for extracting timeseries info:

# COMMAND ----------

def get_check_pass_rate_df(weekly_check_information: DataFrame, monthly_check_information: DataFrame) -> DataFrame:
    """
    Calculates the pass rate for each hour of the day, for each check, for the past month and past week.
    
    Args:
        weekly_check_information (DataFrame): DQ check info from the past week
        monthly_check_information (DataFrame): DQ check info from the past month
    Returns:
        pass_info (DataFrame): Pass rate for each hour of the day, based on past week and past month of data
    """
    #Get weekly and monthly pass rates:
    weekly_pass_info = weekly_check_information.groupBy('check_id', 'hour_of_day').agg(
        F.expr("count(status) as total_count"),
        F.expr("count(case when status = true then 1 end) as true_count")
    )

    monthly_pass_info = monthly_check_information.groupBy('check_id', 'hour_of_day').agg(
        F.expr("count(status) as total_count"),
        F.expr("count(case when status = true then 1 end) as true_count")
    )

    weekly_pass_info = weekly_pass_info.withColumn('pass_rate_last_7_days', F.col('true_count')/F.col('total_count')).select('check_id', 'hour_of_day', 'pass_rate_last_7_days').cache()
    monthly_pass_info = monthly_pass_info.withColumn('pass_rate_last_30_days', F.col('true_count')/F.col('total_count')).select('check_id', 'hour_of_day', 'pass_rate_last_30_days').cache()

    #Join weekly and monthly pass rates.
    pass_info = (weekly_pass_info.join(monthly_pass_info, [weekly_pass_info.check_id == monthly_pass_info.check_id, weekly_pass_info.hour_of_day == monthly_pass_info.hour_of_day], 'outer')
                .select(weekly_pass_info.check_id,
                        weekly_pass_info.hour_of_day,
                        weekly_pass_info.pass_rate_last_7_days,
                        monthly_pass_info.pass_rate_last_30_days)).cache()

    pass_info = pass_info.orderBy(pass_info.check_id.desc(), pass_info.hour_of_day.desc())
    pass_info = pass_info.filter(pass_info.check_id.isNotNull())
    display(pass_info)
    return pass_info


# COMMAND ----------

def replace_null_check_pass_info(pass_info: DataFrame) -> None:
    """
    Replace any hours which have no pass rate, with a pass rate of 0
    Result is a temp view called 'pass_info_timeseries', which can be written to a table

    Args:
        pass_info (DataFrame): pass rate information per hour for checks
    Returns:
        None
    """
    #Fill in empty hour values:
    check_ids = pass_info.select('check_id').distinct()
    # Create a DataFrame with all the hours of the day
    hours_df = spark.range(24).select(F.col("id").alias("hour"))

    # Cross join check_ids with hours_df
    check_ids_with_hour = check_ids.crossJoin(hours_df)

    # Rename the columns to match the desired column names
    check_ids_with_hour = check_ids_with_hour.withColumnRenamed("check_id", "check_id_hours")
    display(check_ids)

    #Join to populate all hour values. Null counts are replaced by 0
    pass_info_all_hours = (check_ids_with_hour.join(pass_info,
                            (check_ids_with_hour.check_id_hours == pass_info.check_id) & (check_ids_with_hour.hour == pass_info.hour_of_day),
                            'left')
                        .select('check_id_hours', 'hour', 'pass_rate_last_7_days', 'pass_rate_last_30_days')
                        .withColumnRenamed('check_id_hours', 'check_id')
                        .withColumnRenamed('hour', 'hour_of_day'))
    pass_info_all_hours = pass_info_all_hours.orderBy(pass_info_all_hours.check_id, pass_info_all_hours.hour_of_day).cache()
    pass_info_all_hours = pass_info_all_hours.fillna(0, subset=['pass_rate_last_7_days', 'pass_rate_last_30_days'])  

    display(pass_info_all_hours)
    pass_info_all_hours.createOrReplaceTempView('pass_info_timeseries')
    return None

# COMMAND ----------

def get_dependency_pass_rate(weekly_dependency_pass_info: DataFrame, monthly_dependency_pass_info: DataFrame) -> DataFrame:
    """
    Calculates the pass rate for each hour of the day, for each dependency, for the past month and past week.
    
    Args:
        weekly_dependency_pass_info (DataFrame): DQ dependency info from the past week
        monthly_dependency_pass_info (DataFrame): DQ dependency info from the past month
    Returns:
        dependency_pass_info (DataFrame): Pass rate for each hour of the day, based on past week and past month of data
    """
    #Get weekly and monthly pass rates:
    weekly_dependency_pass_info = weekly_dependency_info.groupBy('dependency_id', 'hour_of_day').agg(
        F.expr("count(overall_status) as total_count"),
        F.expr("count(case when overall_status = true then 1 end) as true_count")
    )

    monthly_dependency_pass_info = monthly_dependency_info.groupBy('dependency_id', 'hour_of_day').agg(
        F.expr("count(overall_status) as total_count"),
        F.expr("count(case when overall_status = true then 1 end) as true_count")
    )

    weekly_dependency_pass_info = weekly_dependency_pass_info.withColumn('pass_rate_last_7_days', F.col('true_count')/F.col('total_count')).select('dependency_id', 'hour_of_day', 'pass_rate_last_7_days').cache()
    monthly_dependency_pass_info = monthly_dependency_pass_info.withColumn('pass_rate_last_30_days', F.col('true_count')/F.col('total_count')).select('dependency_id', 'hour_of_day', 'pass_rate_last_30_days').cache()

    #Join weekly and monthly pass rates.
    dependency_pass_info = (weekly_dependency_pass_info.join(monthly_dependency_pass_info, [weekly_dependency_pass_info.dependency_id == monthly_dependency_pass_info.dependency_id, weekly_dependency_pass_info.hour_of_day == monthly_dependency_pass_info.hour_of_day], 'outer')
                .select(weekly_dependency_pass_info.dependency_id,
                        weekly_dependency_pass_info.hour_of_day,
                        weekly_dependency_pass_info.pass_rate_last_7_days,
                        monthly_dependency_pass_info.pass_rate_last_30_days)).cache()

    dependency_pass_info = dependency_pass_info.orderBy(dependency_pass_info.dependency_id.desc(), dependency_pass_info.hour_of_day.desc())
    dependency_pass_info = dependency_pass_info.filter(dependency_pass_info.dependency_id.isNotNull())

    display(dependency_pass_info)
    return dependency_pass_info

# COMMAND ----------

def replace_null_dependency_pass_info(dependency_pass_info: DataFrame) -> None:
    """
    Replace any hours which have no pass rate, with a pass rate of 0
    Result is a temp view called 'dependency_pass_info_timeseries', which can be written to a table

    Args:
        dependency_pass_info (DataFrame): pass rate info for dependencies
    Returns:
        None
    """
    #Fill in empty hour values:
    dependency_ids = dependency_pass_info.select('dependency_id').distinct()
    # Create a DataFrame with all the hours of the day
    hours_df = spark.range(24).select(F.col("id").alias("hour"))

    # Cross join dependency_id with hours_df
    dependency_ids_with_hour = dependency_ids.crossJoin(hours_df)

    # Rename the columns to match the desired column names
    dependency_ids_with_hour = dependency_ids_with_hour.withColumnRenamed("dependency_id", "dependency_id_hours")
    display(dependency_ids)

    #Join to populate all hour values. Null counts are replaced by 0
    dependency_pass_info_all_hours = (dependency_ids_with_hour.join(dependency_pass_info,
                            (dependency_ids_with_hour.dependency_id_hours == dependency_pass_info.dependency_id) & (dependency_ids_with_hour.hour == dependency_pass_info.hour_of_day),
                            'left')
                        .select('dependency_id_hours', 'hour', 'pass_rate_last_7_days', 'pass_rate_last_30_days')
                        .withColumnRenamed('dependency_id_hours', 'dependency_id')
                        .withColumnRenamed('hour', 'hour_of_day'))
    dependency_pass_info_all_hours = dependency_pass_info_all_hours.orderBy(dependency_pass_info_all_hours.dependency_id, dependency_pass_info_all_hours.hour_of_day).cache()
    dependency_pass_info_all_hours = dependency_pass_info_all_hours.fillna(0, subset=['pass_rate_last_7_days', 'pass_rate_last_30_days'])  

    display(dependency_pass_info_all_hours)
    dependency_pass_info_all_hours.createOrReplaceTempView('dependency_pass_info_timeseries')
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init global params and load dependency info

# COMMAND ----------

# Init global params
set_global_parameters()


# Get logging information about dependencies from two tables:
dependency_info = spark.sql(f"""with cte as (select di.dependency_id,
                           di.info,
                           di.dts,
                           dd.type,
                           dd.name,
                           ROW_NUMBER() OVER (PARTITION BY di.dependency_id
                                       ORDER BY di.dts DESC)
                                       AS row_number
                 from {catalog}.{schema}.data_quality_dependency_info di
                 join {catalog}.{schema}.data_quality_dependency_definition dd
                 on dd.dependency_id = di.dependency_id)

                 select  dependency_id, info, dts, type, name from cte
                 where row_number = 1
                 """).cache()

# UDF to extract parameters from dependency information
udf_get_dependency_parameter = F.udf(get_dependency_parameter, StringType())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create and Merge Presentation layer

# COMMAND ----------

# Parse info:
power_bi_info_parsed = parse_power_bi_json(dependency_info)
databricks_info_parsed = parse_databricks_json(dependency_info)
display(power_bi_info_parsed)
display(databricks_info_parsed)

# COMMAND ----------

# Create presentation layer view:
create_dependency_presentation_layer(databricks_info_parsed, power_bi_info_parsed)

# COMMAND ----------

#Upsert:
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))

dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ${widget.catalog}.${widget.schema}.data_quality_dependency_run_log as target
# MAGIC USING source
# MAGIC ON (target.dependency_id = source.dependency_id AND
# MAGIC     target.type = source.type AND
# MAGIC     target.workflow_id = source.workflow_id AND
# MAGIC     target.status = source.status AND
# MAGIC     target.start_time = source.start_time
# MAGIC )
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     dependency_id = source.dependency_id,
# MAGIC     dts = source.dts,
# MAGIC     type = source.type,
# MAGIC     schedule = source.schedule,
# MAGIC     workflow_id = source.workflow_id,
# MAGIC     status = source.status,
# MAGIC     run_status_flag = source.run_status_flag,
# MAGIC     start_time = source.start_time,
# MAGIC     end_time = source.end_time,
# MAGIC     duration_in_seconds = source.duration_in_seconds,
# MAGIC     error_message = source.error_message
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     dependency_id,
# MAGIC     dts,
# MAGIC     type,
# MAGIC     schedule,
# MAGIC     name,
# MAGIC     workflow_id,
# MAGIC     status,
# MAGIC     run_status_flag,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     duration_in_seconds,
# MAGIC     error_message
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.dependency_id,
# MAGIC     source.dts,
# MAGIC     source.type,
# MAGIC     source.schedule,
# MAGIC     source.name,
# MAGIC     source.workflow_id,
# MAGIC     source.status,
# MAGIC     source.run_status_flag,
# MAGIC     source.start_time,
# MAGIC     source.end_time,
# MAGIC     source.duration_in_seconds,
# MAGIC     source.error_message
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Display results of merge:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from ${widget.catalog}.${widget.schema}.data_quality_dependency_run_log
# MAGIC order by dts desc
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Timeseries Information on data quality checks:

# COMMAND ----------

# Get weekly and monthly information
weekly_check_information = spark.sql(f'select * from {catalog}.{schema}.data_quality_check_status where cast(dts as date) > DATE_ADD(current_date(), -7)').withColumn('hour_of_day', F.hour('dts')).cache()
monthly_check_information = spark.sql(f'select * from {catalog}.{schema}.data_quality_check_status where cast(dts as date) > DATE_ADD(current_date(), -30)').withColumn('hour_of_day', F.hour('dts')).cache()

# Combine and format into pass rate:
pass_info = get_check_pass_rate_df(weekly_check_information, monthly_check_information)
# Fill any hours which have no info, dafault is to fill with pass rate of 0
replace_null_check_pass_info(pass_info)

# COMMAND ----------

#Insert into table:
display(spark.sql(f"""INSERT OVERWRITE {catalog}.{schema}.data_quality_check_status_timeseries TABLE pass_info_timeseries;"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${widget.catalog}.${widget.schema}.data_quality_check_status_timeseries
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timeseries Information on dependencies

# COMMAND ----------

#Get weekly and monthly pass info
weekly_dependency_info = spark.sql(f'select * from {catalog}.{schema}.data_quality_dependency_status_log where cast(dts as date) > DATE_ADD(current_date(), -7)').withColumn('hour_of_day', F.hour('dts')).cache()
monthly_dependency_info = spark.sql(f'select * from {catalog}.{schema}.data_quality_dependency_status_log where cast(dts as date) > DATE_ADD(current_date(), -30)').withColumn('hour_of_day', F.hour('dts')).cache()

#Combine and format into pass rate:
dependency_pass_info = get_dependency_pass_rate(weekly_dependency_info, monthly_dependency_info)
#Fill any hours which have no pass rate, default is to fill with 0
replace_null_dependency_pass_info(dependency_pass_info)

# COMMAND ----------

#Insert into table:
display(spark.sql(f"""INSERT OVERWRITE {catalog}.{schema}.data_quality_dependency_status_timeseries TABLE dependency_pass_info_timeseries;"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${widget.catalog}.${widget.schema}.data_quality_dependency_status_timeseries
# MAGIC limit 10
