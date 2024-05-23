import datetime
import requests
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import Optional, List, Protocol
from logging import Logger


# Workflow Schedules -----------------------------------------

class WorkflowSchedule(Protocol):
    """
    Base class for implementing different workflow protocols.

    Methods:
    - set_params(input_dataframe: DataFrame, tolerance: float): Sets the input dataframe and tolerance for the metric.
    - calc_cutoff() -> DataFrame: Calculates the cutoff using the specified metric and returns the result as a DataFrame.
    """
    def set_params(self, last_run_ts: Optional[datetime.datetime], last_run_outcome: bool, logger: Logger):
        """Set parameters for workflow schedule"""
        self.last_run_ts = last_run_ts
        self.last_run_outcome = last_run_outcome
        self.logger = logger

    def is_run_required(self) -> DataFrame:
        """Decide whther refresh is required ."""
        raise NotImplementedError()

class Daily(WorkflowSchedule):
    """
    Class to evaluate whether a report needs to be refreshed or not based on a daily schedule

    Methods:
    - _is_any_run_ts(): Check if there is a value for last_run_ts
    - is_run_required(): Check if run is required based on schedule logic
    """

    def _is_any_run_ts(self) -> bool:
        """Check if there is any run timestamp"""
        if self.last_run_ts is None:
            self.logger.info('No refresh history')
            return False
        else:
            self.logger.info('Last Refreshed: {0}'.format(self.last_run_ts.strftime('%Y-%m-%d %H:%M:%S')))
            return True
        
    def is_run_required(self) -> bool:
        """
        Runs logical checks to determine whether report is ready for refresh
        """
        #Check if last run was successful
        if self.last_run_outcome == False:
            self.logger.info('Last run failed.')
            return True
        #Check if there is any last run ts
        if self._is_any_run_ts() == False:
            return True
        # Now check if last run was today:
        if self.last_run_ts.date() == datetime.datetime.now(datetime.timezone.utc).date():
            self.logger.info('Has run today. Last run was: {0}'.format(self.last_run_ts.strftime('%Y-%m-%d %H:%M:%S')))
            return False
        else:
            self.logger.info('Has not run today. Last run was: {0}'.format(self.last_run_ts.strftime('%Y-%m-%d %H:%M:%S')))
            return True

class NoSchedule(WorkflowSchedule):
    """
    Class to handle workflows with no set schedule

    Methods:
    - is_run_required(): Check if run is required based on schedule logic
    """
    
    def is_run_required(self) -> bool:
        """Always run workflows with no schedule"""
        return True

class TwiceDaily(WorkflowSchedule):
    """
    Class to evaluate whether a report neeeds to be refreshed based on a twice-daily schedule.
    Currently the cuttoff point for 'Twice daily' is at 1pm

    Methods:
    - _is_any_run_ts(): Check if there is a value for last_run_ts
    - _is_run_ts_today(): Check if the run ts was today
    - _is_run_ts_hour_before_cutoff: Check if run ts hour is before cutoff
    - _is_current_time_before_cutoff: Check if current time is before cutoff
    - is_run_required(): Check if run is required based on schedule logic
    """
    def __init__(self):
        self.cuttoff_time = 13
        
    def _is_any_run_ts(self) -> bool:
        """Check if there is any run timestamp"""
        if self.last_run_ts is None:
            self.logger.info('No refresh history')
            return False
        else:
            self.logger.info('Last Refreshed: {0}'.format(self.last_run_ts.strftime('%Y-%m-%d %H:%M:%S')))
            return True
        
    def _is_run_ts_today(self, current_ts =  datetime.datetime.now(datetime.timezone.utc)):
        if self.last_run_ts.date() == current_ts.date():
            self.logger.info("Ran today. Last run on {0}".format(self.last_run_ts.strftime('%Y-%m-%d %H:%M:%S')))
            return True
        else:
            self.logger.info("No run today. Last run on {0}".format(self.last_run_ts.strftime('%Y-%m-%d %H:%M:%S')))
            return False
    
    def _is_run_ts_hour_before_cutoff(self):
        if self.last_run_ts.hour < self.cuttoff_time:
            return True
        else:
            return False

    def _is_current_time_before_cutoff(self, current_ts = datetime.datetime.now(datetime.timezone.utc)):
        """
        Args:
            current_ts (datetime): default value is the current time, but can be set manually for testing purposes
        """
        if current_ts.hour < self.cuttoff_time:
            return True
        else:
            return False
        
    def is_run_required(self, current_ts = datetime.datetime.now(datetime.timezone.utc)):
        """
        Runs logical checks to determine whether report is ready for refresh
        Args:
            current_ts (datetime): default value is the current time, but can be set manually for testing purposes
        """
        #Check if last run was successful
        self.logger.info("Current time: {0}".format(current_ts.strftime('%Y-%m-%d %H:%M:%S')))
        if self.last_run_outcome == False:
            self.logger.info('Last run failed.')
            return True
        #Check if there is any last run ts
        if self._is_any_run_ts() == False:
            return True
        # Now check if last run was today:
        if self._is_run_ts_today(current_ts) == False:
            return True
        #If current time is > cutoff, last run should be > cutoff
        #If current time is < cutoff, last run should be < cutoff
        if (self._is_current_time_before_cutoff(current_ts) == self._is_run_ts_hour_before_cutoff()):
            return False
        else:
            return True





def get_workflow_schedule_obj(workflow_schedule_type: str) -> WorkflowSchedule:
    """
    Function to parse the schedule type and return the correct WorkflowSchedule object

    Args:
        workflow_schedule_type (str): The type of schedule, normally defined in populate_data_quality_checks
    Returns:
        (WorkflowSchedule): Correct object for the relevant schedule
        
    """
    if workflow_schedule_type == 'Daily':
        return Daily()
    elif workflow_schedule_type == "":
        return NoSchedule()
    elif workflow_schedule_type == "Twice Daily":
        return TwiceDaily()
    else:
        raise Exception('Unexpected schedule type')






#Code for making Databricks API Call ----------------------------------------

def get_run_list_request(job_id, token: str, databricks_url: str, page_token=None) -> dict:
    """
    Functions uses an API GET request to return a list of all jobs in the workspace.

    Args:
        job_id (): job_id for GET request.
        token (str): Databricks access token
        databricks_url (str): Databricks url
        page_token (str): The specific page to open.
    Returns:
        dict: all job run infomation.
    Raises:
        Exception: Error if the GET request is not successful
    """
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
    
    #Handle error in response codes
    if (response.status_code == 400 
        or response.status_code == 401
        or response.status_code == 403
        or response.status_code == 404
        or response.status_code == 429
        or response.status_code == 500):
        raise Exception("Error code: {0} in http GET Request for run list.".format(response.status_code))
    
    return response.json()


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
    #Set params and submit GET request 
    header = {'Authorization': 'Bearer {}'.format(token)}
    endpoint = '/api/2.1/jobs/list'
    response = requests.get(
        databricks_url + endpoint,
        json=params,
        headers=header
        )
    
    #Handle any error in resonse:
    if (response.status_code == 401 
        or response.status_code == 429 
        or response.status_code == 500):
        raise Exception("Error code: {0} in http GET Request for job list.".format(response.status_code))
    else:
        return response.json()

def get_job_run_dict(job_id: int,
                    token: str,
                    databricks_url: str,
                    next_page_token: Optional[str]):
    """
    Returns the dictonary response from the GET request. Handles the next page token

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
    

def get_latest_run_dict(job_id: int, token: str, databricks_url:str) -> Optional[dict]:
    """
    Gets a dictionary of the information of the latest job run, if the job has been run successfully.

    Args:
        job_id (int): The ID of job we are interested in
        token (str): databricks access token
        databricks_url (str): Databricks access url
    Returns:
        most_recent_run (Optional[dict]): If there has been a successful run, it will be returned
    Raises:
        Exception: Error if end of function is reached and nothing has been returned yet.
    """
    #Get list of runs of the job:
    job_run_dict, next_page_token = get_job_run_dict(job_id, token, databricks_url, None)
    #Initialise variables for search for latest run.
    latest_run_found = False
    i = 0
    most_recent_run = None

    if len(job_run_dict['runs']) == 0:
        #Job has never been run
        return None

    while latest_run_found is False:
        #Loop through list until the most recent run is found:
        state = job_run_dict['runs'][i]['state']['life_cycle_state']

        if state == 'TERMINATED':
            #If job has finished, then latest run is the last successful run
            if job_run_dict['runs'][i]['state']['result_state'] == 'SUCCESS':
                most_recent_run = job_run_dict['runs'][i]
                latest_run_found = True
        
        #If we have reached end of page handle outcome:
        if i == len(job_run_dict['runs']) - 1 and latest_run_found == False:
            if next_page_token is not None:
                #No jobs have been run successfully on this page, so get next page
                job_run_dict, next_page_token = get_job_run_dict(job_id, token, databricks_url, next_page_token)
            else:
                #Job has never been run successfully so return None
                return None

        i+=1
    
    if latest_run_found or most_recent_run is None:
        return most_recent_run
    else:
        raise Exception('End of control sequence reached in get_latest_run_dict without a result being returned. This should not be possible.')

def get_latest_run_ts(job_id, token, databricks_url) -> Optional[datetime.datetime]:
    """
    Extracts the timestamp from the latest job run

    Args:
        job_id (int): The job ID we want the 
        token(str): The databricks access token
        databricks_url(str): The databricks access url
    Returns:
        start_ts (optional(datetime.datetime)): The latest successful run timestamp if it exists
    
    """
    latest_run = get_latest_run_dict(job_id, token, databricks_url)
    if latest_run is None:
        #Job has not been run successfully
        print('None')
        return None
    else:
        start_time = int(latest_run['start_time']) #the time in ms since  1/1/1970 UTC
        reference_time = datetime.datetime(1970,1,1, tzinfo=datetime.timezone.utc)
        time_difference = datetime.timedelta(microseconds=start_time*1000)
        start_ts = reference_time + time_difference
    return start_ts


def get_job_list(token:str, databricks_url:str) -> List[dict]:
    """
    Function to get a list of information of all jobs in the current workspace.

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
        json_out = get_job_list_request(params, token, databricks_url)
        loop_complete = not(json_out['has_more'])
        if loop_complete == False:
            #Next page token:
            page_token = json_out['next_page_token']
        
        if len(job_list) == 0:
            #create job_list
            job_list = json_out['jobs']
        else:
            #add to job_list
            job_list = list(np.hstack((job_list, json_out['jobs'])))

    return job_list


def get_job_id(job_name, token, databricks_url):
    """
    Get job id of the job matching the job name.

    Args:
        job_name (str): The job name
        token (str): The databricks access token
        databricks_url (str): The databricks url
    Returns:
        (int): The job id
    Raises:
        Exception: If more than one job matches the job_name
        Exception: If no jobs match the job_name
    """
    #Get list of all jobs
    matching_jobs = []
    job_list = get_job_list(token, databricks_url)

    #add any jobs matching job_name to list
    for job in job_list:
        name = str.split(job['settings']['name'], '-')[0]
        if job_name == name and '_manual' not in name:
            matching_jobs.append(job)
    
    #Handle possible errors:
    if len(matching_jobs) > 1:
        raise Exception("More than one job matches the naming pattern: " + job_name + ". Matching names: " + '\n' + '\n'.join(map(str, [x['settings']['name'] for x in matching_jobs])))
    elif len(matching_jobs) == 0:
        raise Exception("No jobs match the naming pattern: " + job_name)
    else:
        return int(matching_jobs[0]['job_id'])











