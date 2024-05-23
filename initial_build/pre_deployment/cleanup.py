# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: cleanup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up databricks bundle files.
# MAGIC
# MAGIC Any file listed below will be deleted if it exists. The path should be relative to the bundle folder.
# MAGIC

# COMMAND ----------

#If your file is a databricks notebook, do not add a .py extention
files_to_delete = ['jobs/foundations_v1_job_activity_funnel_current_week.yml',
                   'jobs/foundations_v1_job_activity_time_series.yml',
                   'jobs/foundations_v1_job_nursery_time_series.yml',
                   'jobs/foundations_v1_job_active_earners.yml',
                   'initial_build/views/99.vw_data_quality_dependency_status',
                   'initial_build/schemas/create_schema'
                   #'miscellaneous/import_files/loyalty_acct_archive_load.parquet',
                   #'miscellaneous/import_files/opt_in_airship_2024_02_22.parquet'
                ]

# COMMAND ----------

import os
import requests

# COMMAND ----------

def get_workspace_object_request(params:dict, token:str, databricks_url:str) -> dict:
    """
    Functions uses an API GET request to return a list of objects at the path.

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
    endpoint = '/api/2.0/workspace/list'
    response = requests.get(
        databricks_url + endpoint,
        json=params,
        headers=header
        )
    
    if (response.status_code == 400 
        or response.status_code == 401
        or response.status_code == 403
        or response.status_code == 404
        or response.status_code == 429
        or response.status_code == 500):
        raise Exception("Error code: {0} in http POST Request for job list.".format(response.status_code))
    
    return response.json()

# COMMAND ----------

def post_delete_request(obj:str , token:str , databricks_url:str) -> None:
    """
    Function posts an API request to delete object from databricks workspace.

    Args:
        object (str): The id of the job to run.
        token (str): The databricks access token
        databricks_url (str): The databricks url
    Returns:
        None
    Raises:
        Exception: Error if the POST request is not successful.
    """
    #Set parameters:
    header = {'Authorization': 'Bearer {}'.format(token)}
    endpoint = '/api/2.0/workspace/delete'
    params= {'path': obj}
    #Make POST request
    response = requests.post(
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
        raise Exception("Error code: {0} in http POST Request for job list.".format(response.status_code))
    
    return response.json()


# COMMAND ----------

def cleanup_files(files_to_delete:list) -> None:
    """
    Delete any files listed.

    Args:
        directory_path (str): The base directory path
        files_to_delete (list): List of files to delete.
    Returns:
        None
    """

    databricks_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
    full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    directory_path = "/".join(full_path.split("/")[:-3])

    # Delete the files:
    for f in files_to_delete:
        print('Cleanup file: {0}'.format(f))
        #First check if the file exists:
        if (os.path.isfile('/Workspace' + directory_path + "/" + f)):
            print('Attempting to Delete: {0}'.format(f))
            try:
                #Second check for file existing through a different method
                workspace_objects = get_workspace_object_request({'path': directory_path + "/" + f} ,token, databricks_url)

                try:
                    #Check we are deleting just one object:
                    if len(workspace_objects['objects']) == 1:
                        delete = post_delete_request(directory_path + "/" + f, token, databricks_url)
                        if delete == {}:
                            print("Deleted Successfully.")
                        else:
                            print("Object not deleted.")
                    else:
                        print('Attempting to delete more than one resource.')
                except Exception as e:
                    print(e)

            except Exception as err:
                print(err)

           
            
        else:
            print("{0} is not a file in the bundle.".format('/Workspace' + directory_path + "/" + f))

# COMMAND ----------

            


cleanup_files(files_to_delete)
