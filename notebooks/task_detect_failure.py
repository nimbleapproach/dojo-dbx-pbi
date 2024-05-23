# Databricks notebook source
# MAGIC %md
# MAGIC ### Task Value Retrieval Notebook
# MAGIC This notebook retrieves and prints a task value associated with the key "catalog".

# COMMAND ----------

print(dbutils.jobs.taskValues.get(taskKey="task_start", key="catalog"))