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
import logging
import json
from pprint import pformat
from typing import Optional
import re
import time



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


def set_pbi_parameters():
    global pbi_spn_client_id, pbi_spn_secret_scope, pbi_spn_client_secret, pbi_spn_tenant_id
    pbi_spn_client_id = get_param('pbi_spn_client_id','')
    pbi_spn_secret_scope = get_param('secret_scope','')
    pbi_spn_client_secret = dbutils.secrets.get(pbi_spn_secret_scope, 'SPN-amplpbi-client-secret')
    pbi_spn_tenant_id = get_param('pbi_spn_tenant_id',"")

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
 
    # Send POS request to obtain acces3s token
    response = requests.post(token_url, data=data)
    response.close()
    #If response is successful
    if response.status_code == 200:
        token_data = response.json()
        logger.debug("Access token request successful")
        logger.debug(token_data.get("access_token"))
        return token_data.get("access_token")
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
    relative_url = base_url + "groups"
    workspace_id = None
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    response.close()
     
    if response.status_code == 200:
        logger.debug("Workspace ID request successful")
        data = response.json()
        for workspace in data["value"]:
            if workspace["name"] == workspace_name:
                workspace_id = workspace["id"]

    if workspace_id is None:
        logger.error("Workspace: '{0}'. Workspace ID could not be retrieved".format(workspace_name))
        raise Exception("Workspace: '{0}'. Workspace ID could not be retrieved".format(workspace_name))
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
    relative_url = base_url + f"groups/{workspace_id}/datasets"
    dataset_id_list = []
    #Set the GET response using the relative URL
    response = requests.get(relative_url, headers=headers)
    response.close()
     
    if response.status_code == 200:
        logger.debug("PBI dataset ID request successful")
        data = response.json()
        for dataset in data["value"]:
            if dataset["name"] == dataset_name and dataset["isRefreshable"] == True:
                dataset_id_list.append(dataset["id"])

    if len(dataset_id_list) == 0:
        #Dataset was not found:
        logger.error("Dataset: '{0}' could not be found, or is not refreshable.".format(dataset_name))
        raise Exception("Dataset: '{0}' could not be found, or is not refreshable.".format(dataset_name))
    elif len(dataset_id_list) > 1:
        logger.error("Dataset: '{0}' returned more than one dataset ID.".format(dataset_name))
        raise Exception("Dataset: '{0}' returned more than one dataset ID.".format(dataset_name))
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
    
    relative_url = base_url + f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    #Make request:
    params = {}
    params['type'] = 'full'
    response = requests.post(relative_url, headers=headers, params = params)
 
    if response.status_code == 202:
        logger.info(f"Dataset '{pbi_dataset}' refresh has been triggered successfully.")
        return True
    else:
        logger.error(f"{response.status_code}: Failed to trigger dataset '{pbi_dataset}' refresh.")
        raise Exception(f"{response.status_code}: Failed to trigger dataset '{pbi_dataset}' refresh.")

# COMMAND ----------

def refresh_report(catalog:str, base_url: str, access_token: str, report_name: str) -> None:
    """
    Refresh the Data Quality Reporting datasaet on Power BI with the most up-to-date information.

    Args:
        catalog (str): The catalog of the databricks workspace, to determine environment
        base_url (str): The API base url
        access_token (str): The PBI API access token
        report_name (str): The report to be refreshed
    Raises:
        Exception: Error if catalog does not match expected values
    """
    headers = {"Authorization": f"Bearer {access_token}"}

    if catalog == 'custanwo':
        suffix = ' [Dev]'
    elif catalog == 'custstag':
        suffix = ' [Stage]'
    elif catalog == 'custprod':
        suffix = ""
    else:
        raise Exception("Catalog not recognised")


    pbi_workspace = 'LS Eleven - Asda' + suffix

    workspace_id = get_pbi_workspace_id(pbi_workspace, base_url, headers)
    dataset_id = get_pbi_dataset_id(workspace_id, base_url, headers, report_name)

    invoke_pbi_dataset_refresh(workspace_id, dataset_id, report_name, base_url, headers)
    return None

# COMMAND ----------

set_up_logger()
set_global_parameters()
set_pbi_parameters()

base_url = f"https://api.powerbi.com/v1.0/myorg/"
token_url = f"https://login.microsoftonline.com/{pbi_spn_tenant_id}/oauth2/token"

try:
    pbi_access_token = dbutils.jobs.taskValues.get(taskKey = "data_quality_power_bi_refresh_trigger", key = "pbi_access_token", default = " ", debugValue = " ")
    pbi_access_token_invalid_time = dbutils.jobs.taskValues.get(taskKey = "data_quality_power_bi_refresh_trigger", key = "pbi_access_token_invalid_time", default = " ", debugValue = " ")
except:
    pbi_access_token = " "
    pbi_access_token_invalid_time = " "


if pbi_access_token == " " or pbi_access_token_invalid_time == " ":
    logger.info('Access token request due to no value passed in')
    access_token = get_access_token(pbi_spn_client_id, pbi_spn_client_secret)
elif datetime.datetime.strptime(pbi_access_token_invalid_time, "%d/%m/%Y %H:%M:%S") < datetime.datetime.now():
    logger.debug('Invalid time: {0}'.format(pbi_access_token_invalid_time))
    logger.debug('Now: {0}'.format(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
    logger.info("Access token request due to last token being requested over 30 mins ago.")
    access_token = get_access_token(pbi_spn_client_id, pbi_spn_client_secret)
else:
    logger.info("Access token passed in from previous task")
    access_token = pbi_access_token


refresh_report(catalog, base_url, access_token, 'Data Quality Reporting')
# If we add any info to the landing page, then we need to refresh it:
# refresh_report(catalog, base_url, access_token, 'App Landing Page')
