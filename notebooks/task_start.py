# Databricks notebook source
# MAGIC %md
# MAGIC ### Initialization Notebook
# MAGIC This notebook is responsible for initializing task values from a JSON widget input.

# COMMAND ----------

import json

# Get the JSON string from the widget
dbutils.widgets.text("parameters_json", "{}")
json_string = dbutils.widgets.get("parameters_json")

# Try parsing the JSON string
try:
    json_obj = json.loads(json_string)
    
    # Set keys and values from parameters_json as taskValues so they can be accessed by other tasks from the pipeline
    _ = [dbutils.jobs.taskValues.set(key=k, value=v) for item in json_obj for k, v in item.items()]
except json.JSONDecodeError:
    raise ValueError("Invalid JSON string provided in 'parameters_json' widget.")