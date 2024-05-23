# Databricks notebook source
# MAGIC %md
# MAGIC ## Process: Data Quality Power BI Trigger

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Quality Power BI Trigger
# MAGIC This notebook triggers any relevant Power BI reports after the checks have been run.
# MAGIC The report refresh's are triggered and monitored using the Power BI API.
# MAGIC
# MAGIC ###### Input widgets and apply parameters

# COMMAND ----------

import datetime
import requests
import re
import logging
import json
from pprint import pformat
from typing import Optional
import time
from data_quality_workflow_utils import *

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
    logger.setLevel(logging.getLevelName('INFO'))
    global num_api_calls
    num_api_calls = 0 #For optimisation 

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
    global catalog,schema, api_max_retries, api_default_wait_time
    catalog = get_param('catalog','')
    schema = get_param('schema','')
    api_max_retries = get_param('api_max_retries', '')
    api_default_wait_time = get_param('api_default_wait_time', '')

    logger.debug("catalog: {0}".format(catalog))
    logger.debug("schema: {0}".format(schema))
    logger.debug("api_max_retries: {0}".format(api_max_retries))
    logger.debug("api_max_wait_time: {0}".format(api_default_wait_time))

def set_pbi_parameters():
    global pbi_spn_client_id, pbi_spn_secret_scope, pbi_spn_client_secret, pbi_spn_tenant_id
    pbi_spn_client_id = get_param('pbi_spn_client_id',"")
    pbi_spn_tenant_id = get_param('pbi_spn_tenant_id',"")
    pbi_spn_secret_scope = get_param('secret_scope',"")
    pbi_spn_client_secret = dbutils.secrets.get(pbi_spn_secret_scope, 'SPN-amplpbi-client-secret')


# COMMAND ----------

def get_dependency_parameter(dependency_id: int, parameter_name: str):
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
        logger.error(('Error in trying to parse parameters to dictionary.'))
        raise Exception('Error in trying to parse parameters to dictionary.')
    #If the parameter selected is in the dictionry, return it:
    if parameter_name not in parameters:
        logger.error("Parameter {0} not in parameters for dependecy id {1}.".format(parameter_name, dependency_id))
        raise Exception("Parameter {0} not in parameters for dependecy id {1}.".format(parameter_name, dependency_id))
    else:
        logger.debug("Dependency parameter:")
        logger.debug("{0}: {1}".format(parameter_name, parameters[parameter_name]))
        return parameters[parameter_name]

# COMMAND ----------

    
def api_retry_decorator(func):
    def wrapper(*args,**kwargs):
        global api_max_retries, api_default_wait_time
        num_retries = 0
        retry_required = False
        logger.info('Running API call function: {0}'.format(func.__name__))
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.info('First API call failed: {0}'.format(e))
            retry_required = True
            # Check if a wait time has been given in the error message:
            pattern = re.compile('Retry in (.*) seconds')
            retry_time = pattern.findall(str(e))
            if len(retry_time) == 0:
                #No wait time given so use the default wait time
                wait_time = int(api_default_wait_time)
            else:
                wait_time = int(retry_time[0])
            

        while retry_required == True:
            num_retries += 1
            #Now wait the amount of time:
            logger.info('Waiting {0} seconds.'.format(wait_time))
            time.sleep(wait_time)
            logger.info('API Retry number {0}'.format(num_retries))
            logger.info('Running API call function: {0}'.format(func.__name__))
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.info('API Retry {0} failed: {1}'.format(num_retries, e))
                err_message = str(e)
                retry_required = True
                # Check if a wait time has been given in the error message:
                pattern = re.compile('Retry in (.*) seconds')
                retry_time = pattern.findall(str(e))
                if len(retry_time) == 0:
                    #No wait time given so use the default wait time
                    wait_time = int(api_default_wait_time)
                else:
                    wait_time = int(retry_time[0])
    
            if num_retries == int(api_max_retries):
                logger.error('API request did not return a valid response after {0} retries. This is the maximum allowed number of retries. Error: {1}'.format(num_retries, err_message))
                retry_required = False
                raise Exception('API request did not return a valid response after {0} retries. This is the maximum allowed number of retries. Error: {1}'.format(num_retries, err_message))
    return wrapper


# COMMAND ----------

@api_retry_decorator
def get_access_token(pbi_spn_client_id:str,
                     pbi_spn_client_secret:str
                     ) -> str:
    """
    Get Access token using app ID and client secret

    Args:
        pbi_spn_client_id (str): The client ID for authentication
        pbi_spn_client_secret (str): The client secret for auth
        tenant_id (str): The tenant id for auth
    Returns:
        optional[str]: The access token if it can be found
    Raises:
        Error if any error in request.
    """
    # Data Request for Endpoint
    data = {
        "grant_type": "client_credentials",
        "client_id": pbi_spn_client_id,
        "client_secret": pbi_spn_client_secret,
        "resource": "https://analysis.windows.net/powerbi/api",
    }
    global num_api_calls
    # Send POS request to obtain acces3s token
    response = requests.post(token_url, data=data)
    num_api_calls += 1
    #If response is successful
    if response.status_code == 200:
        token_data = response.json()
        logger.debug("Access token request successful")
        logger.debug(token_data.get("access_token"))
        return token_data.get("access_token")
    elif response.status_code == 429:
        logger.error("Error in get_access_token, Too many calls: {0}".format(response.status_code))
        raise Exception(str(response.json()))
    else:
        logger.error("Error in get_access_token: {0}".format(response.status_code))
        response.raise_for_status()
        raise Exception("No access token could be aquired.")

# COMMAND ----------

@api_retry_decorator
def get_pbi_workspace_id(workspace_name:str,
                         base_url:str,
                         headers:dict[str,str]
                         ) -> str:
    """
    Function to get workspace ID based on workspace name

    Args:
        workspace_name (str): The workspace name to get the ID for
        base_url (str): The API base URL
        headers (dict): Authentication headers.
    Returns:
        (int): The workspace ID
    Raises:
        Exception: Error if the workspace ID cannot be retrieved.
    """
    global num_api_calls
    relative_url = base_url + "groups"
    workspace_id = None
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    num_api_calls += 1

    if response.status_code == 200:
        logger.debug("Workspace ID request successful")
        data = response.json()
        for workspace in data["value"]:
            if workspace["name"] == workspace_name:
                workspace_id = workspace["id"]
    elif response.status_code == 429:
        logger.error("Error in get_access_token, Too many calls: {0}".format(response.status_code))
        raise Exception(str(response.json()))


    if workspace_id is None:
        logger.error("Workspace: {0}. Workspace ID could not be retrieved".format(workspace_name))
        raise Exception("Workspace: {0}. Workspace ID could not be retrieved".format(workspace_name))
    else:
        logger.debug("workspace_id: {0}".format(workspace_id))
        return workspace_id

# COMMAND ----------

@api_retry_decorator
def get_pbi_dataset_id(workspace_id:str,
                       base_url:str,
                       headers:dict[str,str],
                       dataset_name:str
                       ) -> str:
    """
    Get the dataset ID based on dataset name

    Args:
        workspace_id (int): The ID for workspace we are searching
        base_url (str): The API base URL
        headers (dict): Authentication headers
        dataset_name (str): The name of dataset to get the ID of
    Returns:
        int: The dataset ID
    Raises:
        Exception: Error if we get more than 1 dataset ID
        Exception: Error if we get no dataset IDs
    """
    global num_api_calls
    relative_url = base_url + f"groups/{workspace_id}/datasets"
    dataset_id_list = []
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    num_api_calls +=1
     
    if response.status_code == 200:
        logger.debug("PBI dataset ID request successful")
        data = response.json()
        for dataset in data["value"]:
            if dataset["name"] == dataset_name and dataset["isRefreshable"] == True:
                dataset_id_list.append(dataset["id"])
    elif response.status_code == 429:
        logger.error("Error in get_access_token, Too many calls: {0}".format(response.status_code))
        raise Exception(str(response.json()))

    if len(dataset_id_list) == 0:
        #Dataset was not found:
        logger.error("Dataset: {0} could not be found, or is not refreshable.".format(dataset_name))
        raise Exception("Dataset: {0} could not be found, or is not refreshable.".format(dataset_name))
    elif len(dataset_id_list) > 1:
        logger.error("Dataset: {0} returned more than one dataset ID.".format(dataset_name))
        raise Exception("Dataset: {0} returned more than one dataset ID.".format(dataset_name))
    else:
        logger.debug("dataset_id: {0}".format(dataset_id_list[0]))
        return dataset_id_list[0]

# COMMAND ----------

@api_retry_decorator
def invoke_pbi_dataset_refresh(workspace_id:str,
                               dataset_id: str,
                               pbi_dataset: str,
                               base_url: str,
                               headers:dict[str, str]
                               ) -> bool:
    """
    Refresh a Power BI dataset

    Args:
        workspace_id (int): Workspace which contains the report
        dataset_id (int): The dataset we want to refresh
        base_url (str): API base string
        headers (dict): Authorisation headers
    Returns:
        (dict): The results of the POST request
    Raises:
        Exception: Error if POST request is not successful
    """
    global num_api_calls
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    #Make request:
    params = {}
    params['type'] = 'full'
    response = requests.post(relative_url, headers=headers, params=params)
    num_api_calls += 1
 
    if response.status_code == 202:
        logger.info(f"Dataset {pbi_dataset} refresh has been triggered successfully.")
        return True
    elif response.status_code == 429:
        logger.error("Error in get_access_token, Too many calls: {0}".format(response.status_code))
        raise Exception(str(response.json()))
    else:
        logger.error(f"{response.status_code}: Failed to trigger dataset {pbi_dataset} refresh.")
        raise Exception(f"{response.status_code}: Failed to trigger dataset {pbi_dataset} refresh.")

# COMMAND ----------

@api_retry_decorator
def get_pbi_last_refresh_info(report_info: dict,
                              headers:dict[str,str]
                              ) -> dict:
    """
    Get the latest refresh status for desired Power BI

    Args:
        report_info (dict): Basic information on the report, to be populated
        headers (dict): Authentication Headers.
    Returns:
        dict: information about the latest power BI refresh.
    Raises:
        Exception: Error in GET requests.
    """
    global num_api_calls
    #Reset values to make sure recorded results are recent
    logger.info('Getting refresh info for dependency id {0}'.format(report_info['dependency_id']))
    report_info['refresh_id'] = None
    report_info['refresh_status'] = None
    report_info['refresh_start_time'] = None
    report_info['refresh_type'] = None
    report_info['refresh_status'] = None
    report_info['refresh_end_time'] = None
    report_info['refresh_duration_in_seconds'] = None

    workspace_id = report_info['workspace_id']
    dataset_id = report_info['dataset_id']
    #top gets only the last refresh
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"

    response = requests.get(relative_url, headers=headers) #check status
    num_api_calls += 1

    if response.status_code == 202 or response.status_code == 200:
        logger.debug("PBI last refresh information request successful.")
        if len(response.json()['value']) == 0:
            # Report has not been refreshed before
            return report_info
        else:
            last_refresh = response.json()["value"][0]
            
            report_info['refresh_id'] = last_refresh['id']
            report_info['refresh_status'] = last_refresh['status']
            try:
                report_info['refresh_start_time'] = datetime.datetime.strptime(last_refresh['startTime'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                report_info['refresh_start_time'] = datetime.datetime.strptime(last_refresh['startTime'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')

            report_info['refresh_type'] = last_refresh['refreshType']
            if report_info['refresh_status'] == "Completed" or report_info['refresh_status'] == "Failed":
                try:
                    report_info['refresh_end_time'] = datetime.datetime.strptime(last_refresh['endTime'], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S')
                except Exception as e:
                    report_info['refresh_end_time'] = datetime.datetime.strptime(last_refresh['endTime'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                
                report_info['refresh_duration_in_seconds'] = (datetime.datetime.strptime(report_info['refresh_end_time'], '%Y-%m-%d %H:%M:%S') - datetime.datetime.strptime(report_info['refresh_start_time'], '%Y-%m-%d %H:%M:%S')).total_seconds()

            logger.debug("Power BI Refresh Info:")
            logger.debug(pformat(report_info))

            return report_info
    elif response.status_code == 429:
        logger.error("Error in get_access_token, Too many calls: {0}".format(response.status_code))
        raise Exception(str(response.json()))
    else:
        logger.error("{0}: Error in getting dataset last refresh.".format(response.status_code))
        raise Exception("{0}: Error in getting dataset last refresh.".format(response.status_code))

# COMMAND ----------

def run_pbi_refresh(report_info_dict: dict,
                    access_token: str) -> None:
    """
    Run the PBI refresh if it is required for the provided dependency.

    Args:
        report_info_dict: dict
        access_token (str): The access token for authentication
    Returns:
        None
    Raises:
        Exception: Error if dataset refresh status is unexpected.
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    pbi_dataset = report_info_dict['dataset_name']
    workspace_id = report_info_dict['workspace_id']
    dataset_id = report_info_dict['dataset_id']

    logger.info('Checking last refresh info for dataset: {0}'.format(pbi_dataset))
    #Check if report needs refreshing:
    if report_info_dict['refresh_status'] is None:
        last_refresh_ts = None
        last_refresh_result = False
    elif report_info_dict['refresh_status'] == 'Failed' or report_info_dict['refresh_status'] == 'Cancelled':
        last_refresh_ts = datetime.datetime.strptime(report_info_dict['refresh_start_time'], '%Y-%m-%d %H:%M:%S')
        last_refresh_result = False
    elif report_info_dict['refresh_status'] == 'Completed':
        last_refresh_ts = datetime.datetime.strptime(report_info_dict['refresh_start_time'], '%Y-%m-%d %H:%M:%S')
        last_refresh_result = True
    elif report_info_dict['refresh_status'] == 'Unknown':
        last_refresh_ts = datetime.datetime.strptime(report_info_dict['refresh_start_time'], '%Y-%m-%d %H:%M:%S')
        logger.info("Dataset: {0} is currently being refreshed.".format(pbi_dataset))
        last_refresh_result = True
    else:
        logger.error("Dataset: {0}. Unexpected refresh status: {1}".format(pbi_dataset, report_info_dict['refresh_status']))
        raise Exception("Dataset: {0}. Unexpected refresh status: {1}".format(pbi_dataset, report_info_dict['refresh_status']))

    schedule = report_info_dict['schedule_type']

    scheduleType = get_workflow_schedule_obj(schedule)
    scheduleType.set_params(last_run_ts = last_refresh_ts,
                            last_run_outcome = last_refresh_result,
                            logger = logger)
    
    if scheduleType.is_run_required() == False:
        logger.info('Dataset: {0} does not need refreshing.'.format(pbi_dataset))
    elif report_info_dict['refresh_start_time'] is not None:
        logger.info('Dataset: {0} last refresh was: {1}'.format(pbi_dataset, report_info_dict['refresh_start_time']))
        invoke_pbi_dataset_refresh(workspace_id, dataset_id, pbi_dataset, base_url, headers)
    else:
        logger.info('Dataset: {0} has not been refreshed before'.format(pbi_dataset))
        invoke_pbi_dataset_refresh(workspace_id, dataset_id, pbi_dataset, base_url, headers)

    return None

# COMMAND ----------

def log_pbi_last_refresh_info(report_info: dict, access_token:str) -> dict:
    """
    Record last refresh information, regardless of whether the report is ready to refresh or not.

    Args:
        report_info (dict): Information on all PBI Reports
        access_token (str): access token for authentication
    Returns:
        None
    """
    logger.info("Logging info for dependency ID {0}".format(report_info['dependency_id']))
    headers = {"Authorization": f"Bearer {access_token}"}

    if report_info['error_message'] is None or report_info['error_message'] == 'Report was not ready for refresh. Some DQ check has failed.':
        try:
            report_info = get_pbi_last_refresh_info(report_info, headers)
        except Exception as e:
            logger.error('Error in getting last refresh info for dependency ID {0}: {1}'.format(report_info['dependency_id'], e))
            report_info['error_message'] = str(e)

    if report_info['refresh_status'] is None and report_info['error_message'] is None:
        logger.info("No latest run info found for ID: {0}.".format(report_info['dependency_id']))#


    json_string = json.dumps(report_info)

    spark.sql(f"""
        MERGE INTO {catalog}.{schema}.data_quality_dependency_info AS target 
        USING (SELECT {report_info['dependency_id']} AS dependency_id, '{json_string}' AS info,current_timestamp() AS dts) AS source
        ON source.dependency_id = target.dependency_id
        AND source.info = target.info
        WHEN MATCHED THEN
        UPDATE SET target.dts = source.dts
        WHEN NOT MATCHED THEN
        INSERT *
        """)
    logger.debug(pformat(report_info))
    return report_info

# COMMAND ----------

def get_workspace_id_mapping(workspace_names: list, access_token:str) -> dict:
    """
    Maps unique workspace names to a workspace ID to reduce number of API calls required

    Args:
        workspace_names (list): List of all workspace names
        access_token (str): PBI API access token

    Returns:
        mapping_dict (dict): Mapping of workspace names to ID
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    mapping_dict = {}
    for name in list(set(workspace_names)):
        try:
            workspace_id = get_pbi_workspace_id(name, base_url, headers)
            mapping_dict[name] = workspace_id
        except:
            mapping_dict[name] = None
            logger.error('No workspace ID found for {0}'.format(name))
    return mapping_dict


# COMMAND ----------

def create_report_info_dict(dependency_id: int, access_token:str, report_ready: bool, workspace_id_mapping: dict) -> dict:
    """
    Get Information from API calls that will be used multiple times.

    Args:
        dependency_id: The report Dependency ID
        access_token: The API Access token
        report_ready: Is the report ready for refresh
        workspace_id_mapping (dict): Mapping of workspace IDs to workspace names
    Return:
        report_info: A dictionary of all information on the report
    """

    headers = {"Authorization": f"Bearer {access_token}"}

    report_info = {}
    report_info['dependency_id'] = dependency_id
    report_info['workspace_name'] = None 
    report_info['dataset_name'] = None 
    report_info['schedule_type'] = None 
    report_info['dataset_id'] = None 
    report_info['workspace_id'] = None 
    report_info['refresh_id'] = None 
    report_info['refresh_status'] = None
    report_info['refresh_start_time'] = None
    report_info['refresh_end_time'] = None
    report_info['refresh_duration_in_seconds'] = None
    report_info['refresh_type'] = None
    report_info['error_message'] = None

    if report_ready == False:
        report_info['error_message'] = 'Report was not ready for refresh. Some DQ check has failed.'

    try:
        pbi_workspace = get_dependency_parameter(dependency_id,'workspace')
        pbi_dataset = get_dependency_parameter(dependency_id, 'dataset')
        schedule =  get_dependency_parameter(dependency_id, 'schedule type')

        report_info['workspace_name'] = pbi_workspace
        report_info['dataset_name'] = pbi_dataset
        report_info['schedule_type'] = schedule
    except Exception as e:
        logger.error('Error in create report info dict for getting dependency parameters, ID {0}: {1}'.format(dependency_id, e))
        report_info['error_message'] = str(e)

    try:
        if (report_info['dataset_name'] is not None
            and report_info['workspace_name'] in workspace_id_mapping
            and workspace_id_mapping[report_info['workspace_name']] is not None):

            workspace_id = workspace_id_mapping[report_info['workspace_name']]
            dataset_id = get_pbi_dataset_id(workspace_id, base_url, headers, pbi_dataset)

            report_info['dataset_id'] = dataset_id
            report_info['workspace_id'] = workspace_id
        else:
            logger.error('Error in create report info dict, getting workspace and dataset ID, ID {0}'.format(dependency_id))
            report_info['error_message'] = 'Error in create report info dict, getting workspace and dataset ID, ID {0}'.format(dependency_id)
    except Exception as e:
        logger.error('Error in create_report info dict when getting workspace and dataset ID using API, for dependency {0}: {1}'.format(dependency_id, e))
        report_info['error_message'] = str(e)

    return report_info

def run_all_processes(report_info_dict: dict, access_token: str) -> None:
    """
    Function to run all processes and record errors

    Args:
        report_info_dict (dict): Information about this specific report.
        access_token (str): The access token for PBI API.
    
    """
    try:
        #Log every report info:
       report_info_dict =  log_pbi_last_refresh_info(report_info_dict, access_token)
    except Exception as e:
        logger.error('Error in logging last refresh info for dependency ID {0}: {1}'.format(report_info_dict['dependency_id'], e))
        report_info_dict['error_message'] = str(e)
    
    if report_info_dict['error_message'] is None:
        # Refresh reports that are ready:
        try:
            run_pbi_refresh(report_info_dict, access_token)
        except Exception as e:
            logger.error('Error in invoking pbi refresh for dependency ID {0}: {1}'.format(report_info_dict['dependency_id'], e))
            report_info_dict['error_message'] = str(e)
        #Wait or we may get an error due to successive API calls:
        time.sleep(3)
        report_info_dict = log_pbi_last_refresh_info(report_info_dict, access_token)
    
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run all processes:

# COMMAND ----------

set_up_logger()
set_global_parameters()
set_pbi_parameters()

base_url = f"https://api.powerbi.com/v1.0/myorg/"
token_url = f"https://login.microsoftonline.com/{pbi_spn_tenant_id}/oauth2/token"
access_token = get_access_token(pbi_spn_client_id, pbi_spn_client_secret)

pbi_access_token_invalid_time = datetime.datetime.now() + datetime.timedelta(minutes=30)
pbi_access_token_invalid_time = pbi_access_token_invalid_time.strftime("%d/%m/%Y %H:%M:%S")

dbutils.jobs.taskValues.set(key = 'pbi_access_token', value = access_token)
dbutils.jobs.taskValues.set(key = 'pbi_access_token_invalid_time', value = pbi_access_token_invalid_time)

#get all dependency_status:
dependency_status = spark.sql(f"""
                        WITH cte AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dependency_id 
                                    ORDER BY dts DESC)
                                    AS row_number from {catalog}.{schema}.data_quality_dependency_status)

                        SELECT dd.dependency_id, ds.dependency_name, dd.type, ds.overall_status FROM cte as ds
                        JOIN {catalog}.{schema}.data_quality_dependency_definition as dd on ds.dependency_id = dd.dependency_id
                        WHERE row_number = 1
                        """).cache()

display(dependency_status)


# COMMAND ----------

#Get workflows that are ready to run:

workspace_names = [get_dependency_parameter(x[0], 'workspace') for x in dependency_status.filter("type = 'Power BI Report'").select('dependency_id').collect()]
workspace_mapping_dict = get_workspace_id_mapping(workspace_names, access_token)

not_ready_pbi_reports = (dependency_status.filter("type = 'Power BI Report' AND overall_status = False")
                        .select('dependency_id')).collect()
ready_pbi_reports = (dependency_status.filter("type = 'Power BI Report' AND overall_status = True")
                        .select('dependency_id')).collect()

not_ready_pbi_reports = [x[0] for x in not_ready_pbi_reports]
ready_pbi_reports = [x[0] for x in ready_pbi_reports]

all_report_info_dict = {}

for dependency_id in not_ready_pbi_reports:
    all_report_info_dict[dependency_id] = create_report_info_dict(dependency_id, access_token, False, workspace_mapping_dict)
for dependency_id in ready_pbi_reports:
    all_report_info_dict[dependency_id] = create_report_info_dict(dependency_id, access_token, True, workspace_mapping_dict)

# COMMAND ----------


for dependency_id in list(all_report_info_dict.keys()):
    run_all_processes(all_report_info_dict[dependency_id], access_token)

logger.info("{0} API calls".format(num_api_calls))
