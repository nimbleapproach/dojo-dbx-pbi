# Databricks notebook source
# MAGIC %md
# MAGIC ##Process:    goodle_api

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating widgets
# MAGIC ######Input widgets and apply parameters 

# COMMAND ----------

dbutils.widgets.text('core_catalog', '')
dbutils.widgets.text('catalog', '')
dbutils.widgets.text('schema', 'bi_data_model')

# COMMAND ----------

core_catalog = dbutils.widgets.get("core_catalog")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

display(spark.sql(f"""SELECT '{core_catalog}' AS core_catalog,'{catalog}' AS catalog,'{schema}' AS schema;"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation
# MAGIC ######Pull data from google api

# COMMAND ----------

import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Filter
from google.analytics.data_v1beta.types import FilterExpression
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest


# COMMAND ----------

def get_credentails_file_path() -> str:
    """Return Credential file path
    Returns:
    - str: Value of the fetched parameter.
    """
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    credential_api_path = "/Workspace"+"/".join(notebook_path.split("/")[:-2])+"/miscellaneous/credentials/google_api_credentials.json"
    
    return credential_api_path

# COMMAND ----------

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] =  get_credentails_file_path() #"/Workspace/dvsr/credentials.json"
property_id = "279311691"
start_date = "2021-08-11"
end_date = "today"

# COMMAND ----------

# converts ga4 api response into a dataframe

def ga4_to_df(response):
    dim_len = len(response.dimension_headers)
    metric_len = len(response.metric_headers)
    all_data = []
    for row in response.rows:
        row_data = {}
        for i in range(0, dim_len):
            row_data.update(
                {response.dimension_headers[i].name: row.dimension_values[i].value})
        for i in range(0, metric_len):
            row_data.update(
                {response.metric_headers[i].name: row.metric_values[i].value})
        all_data.append(row_data)
    df = pd.DataFrame(all_data)
    return df


# COMMAND ----------

# engagement metrics

def engagement_report():
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="active7DayUsers"),
            Metric(name="active28DayUsers"),
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
            Metric(name="averageSessionDuration"),
            Metric(name="userEngagementDuration"),
            Metric(name="engagementRate")
        ],
        limit=100000,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
    )
    response = client.run_report(request)
    return ga4_to_df(response)


# COMMAND ----------

# screen metrics

def screen_report():
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date"), Dimension(name="unifiedScreenName")],
        metrics=[
            Metric(name="screenPageViews"),
            Metric(name="screenPageViewsPerSession")
        ],
        limit=100000,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)]
    )
    response = client.run_report(request)
    return ga4_to_df(response)


# COMMAND ----------

# first_opens

def first_opens():
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[
            Metric(name="eventCount")
        ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value="first_open"),
            )
        )
    )
    response = client.run_report(request)
    return ga4_to_df(response)


# COMMAND ----------

# app_removes

def app_removes():
    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property=f"properties/{property_id}",
       dimensions=[Dimension(name="date"), Dimension(name="eventName")],
        metrics=[
            Metric(name="eventCount")
        ],
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimension_filter=FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value="app_remove"),
            )
        )
    )
    response = client.run_report(request)
    return ga4_to_df(response)


# COMMAND ----------

## Save the engagement metrics as csv

# app usage
app_usage_api_df = pd.merge(first_opens()[['date', 'eventCount']], engagement_report(), on='date').rename(columns={"eventCount": "first_opens"}).merge(app_removes()[['date','eventCount']]).rename(columns={"eventCount": "app_removes"})
app_usage_api_df = spark.createDataFrame(app_usage_api_df)
display(app_usage_api_df)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Load
# MAGIC ######Insert into app_usage_api

# COMMAND ----------


app_usage_api_df.createOrReplaceTempView("app_usage_api")
 
spark.sql(f"""INSERT OVERWRITE {catalog}.{schema}.app_usage_api TABLE app_usage_api;""")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Test - app_usage_api
# MAGIC ######Query table, order by latest day.

# COMMAND ----------

display(spark.sql(f"""SELECT * FROM {catalog}.{schema}.app_usage_api ORDER BY date DESC"""))

# starts 20210906, same as dremio

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Load
# MAGIC ######Insert into screen_views

# COMMAND ----------

#spark.createDataFrame(screen_report()).withColumnRenamed("date", "full_date").coalesce(1).write.format("csv").options(header='True').saveAsTable(name = f"custanwo.dvsr.screen_views",
#mode = "overwrite",
#path = 'abfss://business@saasllndanwocusproduks01.dfs.core.windows.net/confidential/dvsr/screen_views')

screen_views_df = spark.createDataFrame(screen_report())
screen_views_df.createOrReplaceTempView("screen_views")
 
spark.sql(f"""
          INSERT OVERWRITE {catalog}.{schema}.screen_views
           SELECT
             date,
             unifiedScreenName,
             screenPageViews,
             screenPageViewsPerSession
           FROM screen_views;
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test - screen_views
# MAGIC ######Query table, order by latest day.

# COMMAND ----------

display(spark.sql(f"""SELECT full_date, SUM(screenPageViews) FROM {catalog}.{schema}.screen_views GROUP BY 1 ORDER BY 1 desc"""))
