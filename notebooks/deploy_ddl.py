# Databricks notebook source
# Importing Libraries
from databricks.sdk import WorkspaceClient
import re
import io
import base64
import os

# COMMAND ----------

# Utility Function: Get Parameters

def get_param(param: str, default: str = "") -> str:
    """Fetches the value of the specified parameter using dbutils.widgets.
    
    Parameters:
    - param (str): Name of the parameter to fetch.
    - default (str): Default value to return.
    
    Returns:
    - str: Value of the fetched parameter.
    """
    dbutils.widgets.text(param, default)
    # in case when widget returns empty string we use default value
    if (value := dbutils.widgets.get(param)) and len(value) > 0:
        return value
    else:
        return default

# COMMAND ----------

# Setting Global Parameters

def set_global_parameters():
    """Set global configuration settings and parameters."""
    global catalog,schema, azure_tenant_id, azure_host, azure_client_id, secret_scope, external_volume_location_name, external_volume_location_path
    catalog = get_param('catalog','')
    schema = get_param('schema','')
    azure_tenant_id = get_param('azure_tenant_id','c8f73ab6-9d96-4a57-8b91-d22c63acec71')
    azure_host = get_param('azure_host','https://adb-4827527652115658.18.azuredatabricks.net')
    azure_client_id = get_param('azure_client_id', '3ab3a0cd-b1a4-4dab-9167-74acfa6eceb4') # default to SP used in ANWO
    secret_scope = get_param('secret_scope','kv-anwocus-prod-uks-01')
    external_volume_location_name = get_param('external_volume_location_name', '')
    external_volume_location_path = get_param('external_volume_location_path', '')

# COMMAND ----------

# Initialize Workspace Client

def initialize_workspace_client():
    """Initialize the Databricks workspace client."""
    return WorkspaceClient(
        host=azure_host,
        azure_client_id=azure_client_id,
        azure_client_secret=dbutils.secrets.get(secret_scope, 'ARM-CLIENT-SECRET'),
        azure_tenant_id=azure_tenant_id
    )

# COMMAND ----------

# Get short path to notebook, which is stored in table
def convert_to_short_path(notebook_path: str) -> str:
    """
    Extract the relevant part of the notebook path.
    
    Parameters:
    - notebook_path (str): The full path of the notebook.
    
    Returns:
    - str: The extracted segment or the original path if none of these segments are found.
    """
    match = re.search(r'(initial_build)(/.*)', notebook_path)
    return "".join(match.groups()) if match else None

# COMMAND ----------

# detect type from file name
def determine_notebook_type(notebook_path: str) -> str:
    """
    Determine the type of the notebook based on its path.
    
    Parameters:
    - notebook_path (str): The full path of the notebook.
    
    Returns:
    - str: The type of the notebook ("function", "view", or "table").
    """
    if "functions" in notebook_path:
        return "function"
    elif "views" in notebook_path:
        return "view"
    elif "post_deployment" in notebook_path:
        return "post_deployment"
    else:
        return "table"

# COMMAND ----------

# Function to Find DDL Notebooks
def find_ddl_notebooks(path: str, w) -> list:
    """Recursively find all notebooks with their name in the specified path.
    
    Parameters:
    - path (str): Path to start the search.
    - w: Databricks workspace client to use for the search.
    
    Returns:
    - list: List of paths to the found ddl notebooks.
    """
    items = w.list(path)
    ddl_notebooks = []

    for item in items:
        # If it's a directory, recurse into it
        if getattr(item.object_type, 'name', None) == 'DIRECTORY':
            ddl_notebooks.extend(find_ddl_notebooks(item.path, w))
        # If it's a file with "ddl" in its name, add it to the 
        elif (getattr(item.object_type, 'name', None) == 'NOTEBOOK'): 
            ddl_notebooks.append(convert_to_short_path(item.path))

    ddl_notebooks = [x for x in ddl_notebooks if x is not None]
    return ddl_notebooks

# COMMAND ----------

def get_ddl_deployment_df(): 
    """
    Returns:
    - DataFrame: return ddl_deplyment table
    """
    try:
        #Get the laest deployment row of each object
        ddl_deployment_df=spark.sql(f"""
                                WITH cte AS (
                                    SELECT object,
                                            deployment_content,
                                            ROW_NUMBER() OVER(PARTITION BY object ORDER BY ID DESC) AS row_number                           
                                            FROM {catalog}.{schema}.ddl_deployment)
                                SELECT object,deployment_content FROM cte where row_number = 1;
        """).toPandas()
    except:
        ddl_deployment_df = spark.sql(f""" SELECT 'n/a' AS object,'n/a' AS deployment_content""").toPandas()
    
    return ddl_deployment_df


# COMMAND ----------

# detect type from file name
def get_notebook_content(directory_path: str, notebook_path: str,workspace_client: WorkspaceClient) -> str:
    """ Get notebook content"""
    notebook_content=w.workspace.export(f"{directory_path}/{notebook_path}",ExportFormat='SOURCE').content
    return notebook_content

# COMMAND ----------

# Function to Sort notebooks
# The sort_key function determines the order in which the notebooks should be processed.
# It prioritizes notebooks based on their naming conventions. For example:
# 1. 'function' and 'view' notebooks are given the lowest priority.
# 2. 'metadata' notebooks are processed first among the data tier notebooks.
# Any other notebooks are given a default low priority to ensure they're processed at the end.
def sort_key(notebook: str) -> tuple:
    """Determine the sort order for notebooks based on naming conventions and version numbers.
    
    Parameters:
    - notebook (str): The name of the notebook.
    
    Returns:
    - tuple: A tuple containing the priority, version number (if any), and the notebook name.
    """
    # Extract version number
    #try:
    #    version = int(notebook.split("_v")[1].split(".")[0])
    #except:
    version = 1

    # Determine priority based on keywords and version number
    priority = (
        1 if "schemas" in notebook else
        2 if "pre_deployment" in notebook else
        3 if "tables" in notebook else
        4 if "functions" in notebook else
        5 if "views" in notebook else
        6 if "post_deployment" in notebook else
        9999)
    return (priority, version, notebook)

# COMMAND ----------

# Function to Run a Notebook
def run_notebook(directory_path: str, notebook_path: str,workspace_client: WorkspaceClient) -> str:
    try:
        result = dbutils.notebook.run(f"{directory_path}/{notebook_path}", timeout_seconds=240, arguments={'catalog': catalog,
                                                                                                           'schema': schema,
                                                                                                           'external_volume_location_name': external_volume_location_name,
                                                                                                           'external_volume_location_path': external_volume_location_path
                                                                                                           })
        print(f"ddl notebook {notebook_path} deployed, run output {result}")

        #Insert into the ddl_deployment table
        #If notebook content into string is required.
        #notebook_content = base64.b64decode(notebook_content.content).decode("utf-8")
        spark.sql(f"""
                    INSERT INTO {catalog}.{schema}.ddl_deployment (object, type, deployment_timestamp,deployment_content)
                    VALUES ('{notebook_path}', '{determine_notebook_type(notebook_path)}',DEFAULT,'{notebook_content}')
                """)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise ValueError(f"Notebook failed: {notebook_path}")

    return result

# COMMAND ----------

# Initialization and Setup
# ------------------------
# Define and initialize global parameters
set_global_parameters()

# Initialize and connect to the Databricks workspace
w = initialize_workspace_client()

# Determine the directory path for the current notebook
full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
directory_path = "/".join(full_path.split("/")[:-2])

# Find and process DDL notebooks
ddl_notebooks = find_ddl_notebooks(f"{directory_path}/", w.workspace)
ddl_deployment_df = get_ddl_deployment_df()
#ddl_notebooks = clean_ddl_notebooks(ddl_notebooks)

# Sort the notebooks for execution
sorted_ddl_notebooks = sorted(ddl_notebooks, key=sort_key)

# COMMAND ----------

# Execution of Notebooks
# ----------------------
for notebook in sorted_ddl_notebooks:
    #check if object is already been deployed
    if os.path.isfile('/Workspace' + directory_path + "/" + notebook):
        notebook_content=get_notebook_content(directory_path,notebook,w) 
        if not len(ddl_deployment_df[(ddl_deployment_df.object == notebook) & (ddl_deployment_df.deployment_content == notebook_content)])>0:
            print(f"Start deploying {notebook}")
            result = run_notebook(directory_path=directory_path, notebook_path=notebook,workspace_client=w)
        else:    
            print(f"ddl notebook {notebook} already deployed. No change found therefore skipped this time")
    else:
        print(f'File {notebook} is not in asset bundle.')
