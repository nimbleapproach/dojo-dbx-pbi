# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Quality Check:  Master Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define functions
# MAGIC ######Input widgets and apply parameters
# MAGIC

# COMMAND ----------

# Importing Libraries
import re
import datetime
import asyncio
import time
import concurrent
import functools
from databricks.sdk import WorkspaceClient
from typing import Optional, List

# COMMAND ----------

def get_param(param: str, default: str = "") -> str:
    """
    Fetches the value of the specified parameter using dbutils.widgets.
    
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

def set_global_parameters() -> None:
    """Set global configuration settings and parameters."""
    global catalog, schema, core_catalog, azure_tenant_id, azure_host, azure_client_id, secret_scope, concurrency_limit
    catalog = get_param('catalog','')
    schema = get_param('schema','')
    core_catalog = get_param('core_catalog', 'coreprod')
    azure_tenant_id = get_param('azure_tenant_id','')
    azure_host = get_param('azure_host','')
    azure_client_id = get_param('azure_client_id', '') 
    secret_scope = get_param('secret_scope','')
    concurrency_limit = get_param('concurrency_limit','')
    return None

# COMMAND ----------

# Initialize Workspace Client
def initialize_workspace_client() -> WorkspaceClient:
    """
    Initialize the Databricks workspace client.

    Args:
        None
    Returns:
        (WorkspaceClient): Databricks workspace client for current workspace
    
    """
    return WorkspaceClient(
        host=azure_host,
        azure_client_id=azure_client_id,
        azure_client_secret=dbutils.secrets.get(secret_scope, 'ARM-CLIENT-SECRET'),
        azure_tenant_id=azure_tenant_id
    )

# COMMAND ----------

# Get short path to notebook, which is stored in table
def convert_to_short_path(notebook_path: str) -> Optional[str]:
    """
    Extract the relevant part of the notebook path.
    
    Args:
        notebook_path (str): The full path of the notebook.
    Returns:
        str: The extracted segment or the original path if none of these segments are found.
    """
    match = re.search(r'(data_quality_notebooks)(/.*)', notebook_path)
    return "".join(match.groups()) if match else None

# COMMAND ----------

def str_is_int(s: str) -> bool:
    """ Function to check whether a str could be an integer"""
    try: 
        int(s)
    except ValueError:
        return False
    else:
        return True

# COMMAND ----------

def find_data_quality_check_notebooks(path: str, w) -> list:
    """Find all notebooks with their name in the specified path. Only searches the data_quality_notebooks folder.
    
    Args:
        path (str): Path to start the search.
        w (WorkspaceClient): Databricks workspace client to use for the search.
    Returns:
        data_quality_check_notebooks (list[str]): List of paths to the data quality check notebooks.
    Raises:
        (Exception): Error is raised if all checks defined and enabled in the table are not able to be run.
    """
    items = w.list(path)
    data_quality_check_notebooks = []

    for item in items:
        # If it's a notebook, add to list. 
        if (getattr(item.object_type, 'name', None) == 'NOTEBOOK'): 
            data_quality_check_notebooks.append(convert_to_short_path(item.path))

    #Make sure we are running all of the tests we want to
    valid_check_ids = (set([int(x[0]) for x in spark.sql(f'SELECT check_id FROM {catalog}.{schema}.data_quality_check_definition WHERE active == True').collect()]))

    data_quality_check_notebooks_filtered = []
    for x in data_quality_check_notebooks:
        if x is not None:
            last_part_of_name = x.split('/')[1].split('_')[-1]

            if (('data_quality_check_' in x) and (str_is_int(last_part_of_name))):
                check_id = int(last_part_of_name)
                if check_id in valid_check_ids:
                    valid_check_ids.remove(check_id)
                    data_quality_check_notebooks_filtered.append(x)

    #If there are some check IDs, not in the notebook list, raise an error.
    if len(valid_check_ids) != 0:
        raise Exception("The following checks were not found in the data_quality_notebooks folder: {0}".format(valid_check_ids))

    return data_quality_check_notebooks_filtered

# COMMAND ----------

def run_notebook(directory_path: str,
                 notebook_path: str,
                 check_id: str
                 ) -> Optional[str]:
    """
    Run the notebook provided
    
    Args:
        directory_path (str): Path of main directory
        notebook_path (str): Path of notebook relative to main directory
    Returns:
        result (Optional[str]): Any result returned from running notebook
    """
    print(f"Running {notebook_path}")
    try:
        result = dbutils.notebook.run(f"{directory_path}/{notebook_path}", 
                                      timeout_seconds=600,
                                        arguments={'catalog': catalog,
                                                   'schema': schema, 
                                                   'core_catalog': core_catalog,
                                                   'check_id': check_id,
                                                   'azure_host': azure_host,
                                                   'job_id': '{{job_id}}'})
        #Print Job URL to allow better debugging
        print(f"Check ID {check_id} complete")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        print(f"Notebook failed: {notebook_path}")
        result = None

    

    return result

# COMMAND ----------

def output_results(check_id:str, catalog:str, schema:str) -> None:
    """
    Write the results from global temp views to the table in schema.

    Args:
        check_id (int): The id of check being run
        catalog (str): The catalog to save results to
        schema (str): The schema to save results to
    """
    check_id = int(check_id)
    try:
        results = spark.sql(f"""
            select check_id, definition, status, error_message, run_url, run_duration_seconds from global_temp.check_results_{check_id}
            """).collect()

        results = [x for x in results[0]]
        for i,x in enumerate(results):
            if x is None:
                results[i] = 'null'

        (spark.sql(f"""
                        INSERT INTO {catalog}.{schema}.data_quality_check_status
                        (check_id, definition, status, error_message, run_url, run_duration_seconds)
                        VALUES ({results[0]}, '{results[1]}', {results[2]}, "{results[3]}", '{results[4]}', {results[5]})
                        """))
    except:
        # Insert False result in case of error:

        definition = spark.sql(f"""
                SELECT definition FROM 
                       {catalog}.{schema}.data_quality_check_definition
                WHERE check_id = {check_id}
                """).first()['definition']

        (spark.sql(f"""
                        INSERT INTO {catalog}.{schema}.data_quality_check_status
                        (check_id, definition, status, error_message, run_url, run_duration_seconds)
                        VALUES ({check_id}, '{definition}',{False}, {'Error in saving results from global view'}, "{None}", '{None}')
                        """))

    return None


# COMMAND ----------

async def run_notebook_async(directory_path:str,
                             notebook:str,
                             sem: asyncio.Semaphore,
                             loop: asyncio.AbstractEventLoop
                             ) -> Optional[str]:
    """
    Async wrapper for run_notebook() function

    Args:
        directory_path (str): Path of main directory
        notebook (str): Path of notebook relative to directory
        sem (asyncio.Semaphore):The number of notebooks that can be run in parallel
    Returns:
        result (): The result returned from running the notebook

    """
    async with sem:
        #Run notebooks in parallel:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, 
                                                functools.partial(run_notebook, 
                                                                  directory_path = directory_path, 
                                                                  notebook_path=notebook, 
                                                                  check_id = notebook.split('_')[-1]))
            #We now write the results from the temp views. This approach is chosen to be able to get the URLS of failed notebooks for debugging.
            write_results = await loop.run_in_executor(pool, 
                                                functools.partial(output_results,
                                                                  check_id = notebook.split('_')[-1],
                                                                  catalog = catalog,
                                                                  schema = schema))
    return result



# COMMAND ----------

async def run_all_notebooks(directory_path: str,
                            data_quality_check_notebooks: list[str],
                            concurrency_limit: int
                            ) -> List[Optional[str]]:
    """
    Function to run all notebooks asyncronously (in parallel)

    Args:
        directory_path (str): Path of main directory
        data_quality_check_notebooks (list[str]): List of notebooks to be run
        concurrency_limit (int): The number of notebooks that can be run in parallel
    Returns:
        results (List[str | None]): Any returns from running the notebooks.


    """
    loop = asyncio.get_running_loop() #for concurrent notebook running
    sem = asyncio.Semaphore(concurrency_limit) #The number of notebooks that can be run in parallel
    coroutines = [run_notebook_async(directory_path, notebook, sem, loop) for notebook in data_quality_check_notebooks]
    results = await asyncio.gather(*coroutines)
    return results


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Notebooks
# MAGIC ##### Get location of notebooks, run all notebooks

# COMMAND ----------

#Run all processes ---------------

#Init global variables
set_global_parameters()
#Init workspace client
w = initialize_workspace_client()
#Get full path and directory path
full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
directory_path = "/".join(full_path.split("/")[:-2])

# Find and process DDL notebooks
data_quality_check_notebooks = find_data_quality_check_notebooks(f"{directory_path}/data_quality_notebooks/", w.workspace)
#Run notebooks - await is required to run async functions 
results = await run_all_notebooks(directory_path, data_quality_check_notebooks, int(concurrency_limit))



# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC ##### Display result of all checks run

# COMMAND ----------

today_date = datetime.datetime.today().strftime('%Y-%m-%d')
display(spark.sql(f"""with cte as (
                    SELECT *, 
                    ROW_NUMBER() OVER (PARTITION BY check_id 
                                       ORDER BY dts DESC)
                                       AS row_number
                    FROM {catalog}.{schema}.data_quality_check_status
)
                    SELECT * FROM cte
                    WHERE CAST(dts AS date) = CURRENT_DATE() and row_number = 1
                    ORDER BY check_id
                    """))

# COMMAND ----------


