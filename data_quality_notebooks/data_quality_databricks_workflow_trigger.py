# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: data_quality_dependency_trigger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Dependency Trigger
# MAGIC
# MAGIC This notebook triggers any relevant dependencies after the checks have been run.
# MAGIC
# MAGIC The databricks workflows are triggered and monitored using the databricks jobs API.
# MAGIC
# MAGIC ######Input widgets and apply parameters
# MAGIC

# COMMAND ----------

import datetime
import requests
import logging
import json
import numpy as np
from pprint import pformat
from typing import Optional, List
import time
from data_quality_workflow_utils import *

num_api_calls = 0


# COMMAND ----------

def set_up_logger():
    """Sets up logger to record flow of program."""
    global logger
    logging.basicConfig(format=('%(asctime)s: '
                                '%(filename)s: '    
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s'))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    

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
    logger.debug("catalog: {0}".format(catalog))
    logger.debug("schema: {0}".format(schema))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all dependency information:

# COMMAND ----------

def get_dependency_parameter(dependency_id: int, parameter_name:str) -> str:
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

    parameters_df = spark.sql(f"""
          SELECT parameter FROM {catalog}.{schema}.data_quality_dependency_definition
          WHERE dependency_id = {dependency_id}
          """).cache()
    
    #Check that we have just 1 paramter column
    if parameters_df.count() > 1:
        logger.error("More than one set of parameters found for dependency ID {0}".format(dependency_id))
        raise Exception("More than one set of parameters found for dependency ID {0}".format(dependency_id))
    if parameters_df.count() == 0:
        logger.error("No parameters found for dependency ID {0}".format(dependency_id))
        raise Exception("No parameters found for dependency ID {0}".format(dependency_id))
    
    #Read the paramter column value as a dictionary
    try:
        parameters = eval(parameters_df.collect()[0][0])
    except Exception as e:
        logger.error('Error in trying to parse parameters to dictionary.')
        raise Exception('Error in trying to parse parameters to dictionary.')
    
    #If the parameter selected is in the dictionry, return it:
    if parameter_name not in parameters:
        logger.error("Parameter '{0}' not in parameters for dependecy id {1}.".format(parameter_name, dependency_id))
        raise Exception("Parameter '{0}' not in parameters for dependecy id {1}.".format(parameter_name, dependency_id))
    else:
        logger.debug("Dependency parameter:")
        logger.debug("{0}: {1}".format(parameter_name, parameters[parameter_name]))
        return parameters[parameter_name]

    
    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Process for running databricks workflow:
# MAGIC #### Process for making API calls:

# COMMAND ----------

def get_job_list_request(params:dict, token:str, databricks_url:str) -> dict:
    """
    Functions uses an API GET request to return a list of all jobs in the workspace.

    Args:
        params (dict): parameters for GET request. Sets the return limit and page number
        token (str): Databricks access token
        databricks_url (str): Databricks url
    Returns:
        list[dict]: List of job information dictionaries
    Raises:
        Exception: Error if the GET request is not successful
    """
    global num_api_calls
    #Set params and submit GET request 
    header = {'Authorization': 'Bearer {}'.format(token)}
    endpoint = '/api/2.1/jobs/list'
    response = requests.get(
        databricks_url + endpoint,
        json=params,
        headers=header
        )
    num_api_calls +=1
    
    #Handle any error in resonse:
    if (response.status_code == 401 
        or response.status_code == 429 
        or response.status_code == 500):
        logger.error("Error code: {0} in http GET Request for job list.".format(response.status_code))
        raise Exception("Error code: {0} in http GET Request for job list.".format(response.status_code))
    else:
        logger.debug("job list GET request successful")
        return response.json()

# COMMAND ----------

def get_run_list_request(job_id, token, databricks_url, page_token=None) -> dict:
    """
    Functions uses an API GET request to return a list of all runs for the chosen job in the workspace.

    Args:
        job_id (dict): job_id for GET request.
        token (str): Databricks access token
        databricks_url (str): Databricks url
    Returns:
        dict: all job run infomation.
    Raises:
        Exception: Error if the GET request is not successful
    """
    global num_api_calls
    header = {'Authorization': 'Bearer {}'.format(token)}
    endpoint = '/api/2.1/jobs/runs/list'
    #default is to request the first page.
    if page_token is None:
        #Get first page
        params= {'job_id': job_id}
    else:
        #get the next page
        params = {'job_id':job_id,
                  'page_token':page_token}
    #make the request
    response = requests.get(
        databricks_url + endpoint,
        json=params,
        headers=header
        )
    num_api_calls += 1
    #Handle error in response codes
    if (response.status_code == 400 
        or response.status_code == 401
        or response.status_code == 403
        or response.status_code == 404
        or response.status_code == 429
        or response.status_code == 500):
        logger.error("Error code: {0} in http GET Request for run list.".format(response.status_code))
        raise Exception("Error code: {0} in http GET Request for run list.".format(response.status_code))
    else:
        logger.debug("workflow run list GET request was successful.")
        return response.json()

# COMMAND ----------

def post_job_run_request(job_id:int , token:str , databricks_url:str) -> None:
    """
    Function posts an API request to run a databricks job based on job ID

    Args:
        job_id (int): The id of the job to run.
        token (str): The databricks access token
        databricks_url (str): The databricks url
    Returns:
        None
    Raises:
        Exception: Error if the POST request is not successful.
    """
    global num_api_calls
    #Set parameters:
    header = {'Authorization': 'Bearer {}'.format(token)}
    endpoint = '/api/2.1/jobs/run-now'
    params= {'job_id': job_id}
    #Make POST request
    response = requests.post(
    databricks_url + endpoint,
    json=params,
    headers=header
    )
    num_api_calls += 1
    #Handle error in response codes
    if (response.status_code == 400 
        or response.status_code == 401
        or response.status_code == 403
        or response.status_code == 404
        or response.status_code == 429
        or response.status_code == 500):
        logger.error("Error code: {0} in http GET Request for job list.".format(response.status_code))
        raise Exception("Error code: {0} in http GET Request for job list.".format(response.status_code))
    else:
        logger.debug("job run POST request was successful.")
    return None



# COMMAND ----------

def get_job_run_dict(job_id: int,
                    token: str,
                    databricks_url: str,
                    next_page_token: Optional[str]):
    """
    Returns the response from API request to get list of job runs for chosen job_id. 
    Provides the next page token if there are more job runs.

    Args:
        job_id (int): Id of job we want the latest run time.
        token (str): Databricks access token
        databricks_url (str): Databricks access url
        next_page_token (optional[str]): Token of the next page if it is known
    Returns:
        job_run_dict (dict): The results of GET request
        next_page_token(Optional[str]): The token of the next page if there is one
    """
    job_run_dict = get_run_list_request(job_id, token, databricks_url,next_page_token)
    if job_run_dict['has_more']:
        next_page_token = job_run_dict['next_page_token']
        return job_run_dict, next_page_token
    else:
        return job_run_dict, None

# COMMAND ----------

def get_latest_run_dict(job_info_dict:dict, token: str, databricks_url:str) -> Optional[dict]:
    """
    Returns the dictionary of the latest successful/in progress job run. 
    If there are no successful/in progress jobs then None is returned.

    Args:
        job_info_dict (int): DIctionary of info on the job we are interested in
        token (str): databricks access token
        databricks_url (str): Databricks access url
    Returns:
        most_recent_run (Optional[dict]): If there has been a successful run, it will be returned
    Raises:
        Exception: Error if end of function is reached and nothing has been returned yet.
    """
    #Get list of runs of the job:
    job_id = job_info_dict['job_id']

    job_run_dict, next_page_token = get_job_run_dict(job_id, token, databricks_url, None)
    #Initialise variables for search for latest run.
    latest_run_found = False
    i = 0
    most_recent_run = None
    if 'runs' not in job_run_dict.keys():
        #Job has never been run
        return None
    elif len(job_run_dict['runs']) == 0:
        #Job has never been run
        return None

    while latest_run_found is False:
        #Loop through list until the most recent run is found:
        state = job_run_dict['runs'][i]['state']['life_cycle_state']

        if state == 'RUNNING' or state == 'QUEUED' or state ==  'PENDING' or state == 'TERMINATING':
            #If job is still running, then it is the latest run
            most_recent_run = job_run_dict['runs'][i]
            latest_run_found = True
        elif state == 'TERMINATED':
            #If job has finished, then latest run is the last successful run
            if job_run_dict['runs'][i]['state']['result_state'] == 'SUCCESS':
                most_recent_run = job_run_dict['runs'][i]
                latest_run_found = True

        i+=1
        #If we have reached end of page handle outcome:
        if i == len(job_run_dict['runs']) and latest_run_found == False:
            if next_page_token is not None:
                #No jobs have been run successfully on this page, so get next page
                job_run_dict, next_page_token = get_job_run_dict(job_id, token, databricks_url, next_page_token)
                i=0
            else:
                #Job has never been run successfully so return None
                return None

        
    

    if latest_run_found or most_recent_run is None:
        logger.debug("Most recent run:")
        logger.debug(pformat(most_recent_run))
        return most_recent_run
    else:
        logger.error('End of control sequence reached in get_latest_run_dict without a result being returned. This should not be possible.')
        raise Exception('End of control sequence reached in get_latest_run_dict without a result being returned. This should not be possible.')


# COMMAND ----------

def log_workflow_info(job_info_dict:dict,
                      databricks_url: str,
                      token:str,
                      job_list: list) -> dict:
    """
    Function to log databricks workflow info as json string

    Args:
        job_info_dict (dict): Dictory of info on job we are interested in
        databricks_url (str): The URL of current databricks context
        token (str): The access token of current databricks context
        job_list (list): List of all databricks jobs
    """

    #Handle any errors
    if databricks_url is None:
        logger.error("Databricks url could not be found.")
        raise Exception("Databricks url could not be found.")
    if token is None:
        logger.error("Databricks token could not be found.")
        raise Exception("Databricks token could not be found.")
    
    # set values that we may change to empty so any historic data is not persisted

    job_info_dict['run_id'] = None
    job_info_dict['creator_user_name'] = None
    job_info_dict['number_in_job'] = None
    job_info_dict['original_attempt_run_id'] = None
    job_info_dict['state'] = None
    job_info_dict['result_state'] = None
    job_info_dict['state_message'] = None
    job_info_dict['user_cancelled_or_timedout'] = None
    job_info_dict['start_time'] = None
    job_info_dict['setup_duration'] = None
    job_info_dict['execution_duration'] = None
    job_info_dict['cleanup_duration'] = None
    job_info_dict['end_time'] = None
    job_info_dict['run_duration'] = None
    job_info_dict['trigger'] = None
    job_info_dict['run_name'] = None
    job_info_dict['run_page_url'] = None
    job_info_dict['run_type'] = None
    job_info_dict['format'] = None

    if job_info_dict['workflow_name'] is not None and job_info_dict['error_message'] is None:
        #Get job parameters:
        workflow_name = job_info_dict['workflow_name']
        job_id = job_info_dict['job_id']
        #get latest run
        latest_run = get_latest_run_dict(job_info_dict, token, databricks_url)

        if latest_run is None:
            logger.info("No latest run info found for ID: {0}.".format(dependency_id))#
            job_info_dict['error_message'] = "No latest run info found for ID: {0}.".format(dependency_id)
        else:
            for k in list(latest_run.keys()):
                job_info_dict[k] = latest_run[k]
    else:   
        logger.info('API Job details were not requested for ID {0}'.format(dependency_id))

    

    json_string = json.dumps(job_info_dict)
    spark.sql(f"""
     MERGE INTO {catalog}.{schema}.data_quality_dependency_info AS target 
        USING (SELECT {job_info_dict['dependency_id']} AS dependency_id, '{json_string}' AS info,current_timestamp() AS dts) AS source
        ON source.dependency_id = target.dependency_id
        AND source.info = target.info
        WHEN MATCHED THEN
        UPDATE SET target.dts = source.dts
        WHEN NOT MATCHED THEN
        INSERT *
    """)
    return job_info_dict


# COMMAND ----------

def get_latest_run_ts(job_info_dict:dict) -> Optional[datetime.datetime]:
    """
    Extracts the timestamp from the latest job run

    Args:
        job_info_dict (int): The info of the job we want
        token(str): The databricks access token
        databricks_url(str): The databricks access url
    Returns:
        start_ts (optional(datetime.datetime)): The latest successful run timestamp if it exists
    
    """
    if 'start_time' not in list(job_info_dict.keys()):
        #Job has not been run successfully
        return None
    if job_info_dict['start_time'] is None:
        return None
    else:
        start_time = int(job_info_dict['start_time']) #the time in ms since  1/1/1970 UTC
        reference_time = datetime.datetime(1970,1,1, tzinfo=datetime.timezone.utc)
        time_difference = datetime.timedelta(microseconds=start_time*1000)
        start_ts = reference_time + time_difference
    return start_ts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Process for running databricks workflow:
# MAGIC

# COMMAND ----------

def get_job_list(token:str, databricks_url:str) -> List[dict]:
    """
    Function to get a list of information about all jobs in the current workspace.

    Args:
        token (str): The databricks access token
        databricks_url (str): The databricks url
    Returns:
        job_list (List[dict]): List of information on all jobs in workspace

    """
    #Initiliase parameters
    loop_complete=False
    page_token = None
    job_list = []

    # Continue to submit get requests until we have all jobs
    while loop_complete == False:
        
        if page_token is None:
            #Get first page
            params= {'limit': 100}
        else:
            #get the next page
            params = {'limit':100,
                      'page_token':page_token}
        
        #submit GET request and check if we have all jobs
        response = get_job_list_request(params, token, databricks_url)
        loop_complete = not(response['has_more'])
        if loop_complete == False:
            #Next page token:
            page_token = response['next_page_token']
        
        if len(job_list) == 0:
            #create job_list
            job_list = response['jobs']
        else:
            #add to job_list
            job_list = list(np.hstack((job_list, response['jobs'])))

    return job_list

# COMMAND ----------


def get_job_id(job_name, job_list):
    """
    Get job id of the job matching the job name.

    Args:
        job_name (str): The job name
        job_list (list): list of all databricks jobs
    Returns:
        (int): The job id
    Raises:
        Exception: If more than one job matches the job_name
        Exception: If no jobs match the job_name
    """
    #Get list of all jobs
    matching_jobs = []

    #add any jobs matching job_name to list
    for job in job_list:
        name = str.split(job['settings']['name'], '-')[0]
        if job_name == name and '_manual' not in name:
            matching_jobs.append(job)
    
    #Handle possible errors:
    if len(matching_jobs) > 1:
        logger.error("More than one job matches the naming pattern: " + job_name + ". Matching names: " + '\n' + '\n'.join(map(str, [x['settings']['name'] for x in matching_jobs])))
        raise Exception("More than one job matches the naming pattern: " + job_name + ". Matching names: " + '\n' + '\n'.join(map(str, [x['settings']['name'] for x in matching_jobs])))
    elif len(matching_jobs) == 0:
        logger.error("No jobs match the naming pattern: " + job_name)
        raise Exception("No jobs match the naming pattern: " + job_name)
    else:
        logger.debug("job id: {0}".format(int(matching_jobs[0]['job_id'])))
        return int(matching_jobs[0]['job_id'])



# COMMAND ----------

def get_schedule_status(job_name:str,
                        job_list: list) -> Optional[str]:
    """
    Gets the schedule status of the job. We do not want to run a paused job, only jobs that are unpaused, or have no schedule.

    Args:
        job_name (str): The databricks job name. Must be unique and only match one job
        job_list (list):list of all jobs
    Returns:
        (Optional[str]): The schedule status of the job. None if no schedule. 
    """

    job_dict = None
    matching_jobs = []
    for job in job_list:
        api_job_name = job['settings']['name']
        if job_name == api_job_name and '_manual' not in job['settings']['name']:
            matching_jobs.append(job)

    if len(matching_jobs) == 0:
        raise Exception('No jobs matched the job name: {0}'.format(job_name))
    elif len(matching_jobs) > 1:
        raise Exception('More than one job matched the job name: {0}'.format(job_name))
    elif len(matching_jobs) == 1:
        job_dict = matching_jobs[0]
    if job_dict is None:
        raise Exception('No job was found, this error should not be possible')

    if 'schedule' not in job_dict['settings']:
        #No schedule
        logger.info('{0} has no schedule'.format(job_name))
        return None
    else:
        if job_dict['settings']['schedule']['pause_status'] == 'UNPAUSED':
            logger.info('{0} is unpaused'.format(job_name))
            return 'UNPAUSED'
        elif job_dict['settings']['schedule']['pause_status'] == 'PAUSED':
            logger.info('{0} is paused'.format(job_name))
            return 'PAUSED'

    raise Exception('End of control sequence reached. THis should not be possible. Job dict: {0}'.format(job_dict))


# COMMAND ----------

def run_databricks_workflow(workflow_info_dict: dict,
                            databricks_url: str,
                            token:str,
                            job_list: list
                            ) -> None:
    """
    Run databricks workflow
    Args:
        workflow_info_dict (dict): Info on the job to run
        databricks_url: The Databricks URL of current context
        token: The Databricks API access token
        job_list (list): List of all databricks jobs.
    Returns:
        None
    Raises:
        Exception: If databricks url cannot be found
        Exception: If databricks access token cannot be found
    """

    #Handle any errors
    if databricks_url is None:
        logger.error("Databricks url could not be found.")
        raise Exception("Databricks url could not be found.")
    if token is None:
        logger.error("Databricks token could not be found.")
        raise Exception("Databricks token could not be found.")

    #Get job parameters:
    
    workflow_name = workflow_info_dict['workflow_name']
    schedule_type = workflow_info_dict['schedule_type']
    job_id = workflow_info_dict['job_id']

    logger.info('Workflow: {0}'.format(workflow_name))

    last_run_ts = get_latest_run_ts(workflow_info_dict)

    last_run_result = True
    scheduleType = get_workflow_schedule_obj(schedule_type)
    scheduleType.set_params(last_run_ts = last_run_ts,
                            last_run_outcome=last_run_result,
                            logger=logger)

    if scheduleType.is_run_required() == True:
        logger.info('Run requred')
        post_job_run_request(job_id, token, databricks_url)
    else:
        logger.info('Run not required')

    return None

# COMMAND ----------

def create_workflow_info_dict(dependency_id: int,
                              is_workflow_ready: bool,
                              job_list: list) -> dict:
    """
    Function to aggregate key information about each workflow, to reduce number of API calls required
    
    Args:
        dependency_id (int): The dependency ID
        is_workflow_ready (bool): Is the workflow ready to refresh
        job_list (list): List of all jobs in workspace
    """

    workflow_info_dict = {}
    workflow_info_dict['dependency_id'] = dependency_id
    workflow_info_dict['error_message'] = None
    workflow_info_dict['workflow_name'] = None
    workflow_info_dict['schedule_type'] = None
    workflow_info_dict['job_id'] = None
    workflow_info_dict['workflow_ready_to_run'] = is_workflow_ready

    try:
        workflow_name = get_dependency_parameter(dependency_id,'databricks_workflow_name')
        schedule_type = get_dependency_parameter(dependency_id, 'schedule type')

        workflow_info_dict['workflow_name'] = workflow_name
        workflow_info_dict['schedule_type'] = schedule_type
    except Exception as e:
        logger.error('Error in getting dependency parameters for ID {0}: {1}'.format(dependency_id, e))
        workflow_info_dict['error_message'] = str(e)
    

    if workflow_info_dict['error_message'] is None:
        try:
            job_id = get_job_id(workflow_info_dict['workflow_name'], job_list)
            workflow_info_dict['job_id'] = job_id
        except Exception as e:
            logger.error('Error in getting job ID for Dependency ID {0}: {1}'.format(dependency_id, e))
            workflow_info_dict['error_message'] = str(e)
    
    return workflow_info_dict
    


# COMMAND ----------

set_up_logger()
set_global_parameters()

#Get access token and URL for current context.
databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

#Get list of all jobs on workspace
matching_jobs = []
job_list = get_job_list(token, databricks_url)

#get all dependency_status:
dependency_status = spark.sql(f"""
                         WITH cte AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dependency_id 
                                       ORDER BY dts DESC)
                                       AS row_number from {catalog}.{schema}.data_quality_dependency_status)

                        SELECT dd.dependency_id,ds.dependency_name, dd.type, ds.overall_status FROM cte as ds
                        JOIN {catalog}.{schema}.data_quality_dependency_definition as dd on ds.dependency_id = dd.dependency_id
                        WHERE row_number = 1
                         """).cache()
display(dependency_status)

#Get workflows that are ready to run and not run:
not_ready_databricks_workflows = databricks_workflows = (dependency_status.filter("type = 'Databricks Workflow' AND overall_status = False")
                        .select('dependency_id')).collect()
ready_databricks_workflows = (dependency_status.filter("type = 'Databricks Workflow' AND overall_status = True")
                        .select('dependency_id', 'dependency_name')).collect()
all_databricks_workflows = (dependency_status.filter("type = 'Databricks Workflow'")
                        .select('dependency_id', 'dependency_name')).collect()

# We want jobs without PAUSED status:
not_paused_workflows = []
paused_workflows = []
for workflow in ready_databricks_workflows:
    status = get_schedule_status(workflow[1], job_list)
    if status is None or status == 'UNPAUSED':
        not_paused_workflows.append(workflow)
    else:
        paused_workflows.append(workflow)

not_ready_databricks_workflows = ([(x[0]) for x in not_ready_databricks_workflows])
all_databricks_workflows = ([(x[0]) for x in all_databricks_workflows])
paused_workflows = ([(x[0]) for x in paused_workflows])

not_ready_databricks_workflows = list(np.array(np.hstack((not_ready_databricks_workflows, paused_workflows)), dtype=int))

ready_databricks_workflows = [x[0] for x in not_paused_workflows]

all_workflow_info = {}

#Cast to int neede below to avioud error with numpy int64 type when serialising Json
for dependency_id in not_ready_databricks_workflows:
    all_workflow_info[int(dependency_id)] = create_workflow_info_dict(int(dependency_id), False, job_list)
for dependency_id in ready_databricks_workflows:
    all_workflow_info[int(dependency_id)] = create_workflow_info_dict(int(dependency_id), True, job_list)


# COMMAND ----------

#For all workflows record last run info
for dependency_id in all_databricks_workflows:
    logger.info('Logging ID {0}'.format(dependency_id))
    try:
        all_workflow_info[dependency_id] = log_workflow_info(all_workflow_info[dependency_id], databricks_url, token, job_list)
    except Exception as e:
        logger.error('Error in Logging workflow info for ID {0}: {1}'.format(dependency_id, e))
        all_workflow_info[dependency_id]['error_message'] = str(e)
    
    if (all_workflow_info[dependency_id]['workflow_ready_to_run'] and 
        (all_workflow_info[dependency_id]['error_message'] is None or
         # THis specific error should not stop a refresh
         'No latest run info found for ID:' in all_workflow_info[dependency_id]['error_message'])):
        try:
            logger.info('Dependency ID {0} is eligible to run'.format(dependency_id))
            run_databricks_workflow(all_workflow_info[dependency_id], databricks_url, token, job_list)
        except Exception as e:
             logger.info('Error trying to run databricks workflow Dependency ID {0}: {1}'.format(dependency_id, e))
             all_workflow_info[dependency_id]['error_message'] = str(e)
    
        time.sleep(3)
        all_workflow_info[dependency_id] = log_workflow_info(all_workflow_info[dependency_id], databricks_url, token, job_list)

    
logger.debug('{0} API Calls'.format(num_api_calls))
