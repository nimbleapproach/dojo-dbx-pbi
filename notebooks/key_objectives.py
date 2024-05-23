# Databricks notebook source
# MAGIC %md
# MAGIC # LS Eleven Key Objectives

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating widgets
# MAGIC ######Input widgets and apply parameters 

# COMMAND ----------

dbutils.widgets.text('core_catalog', 'coreprod')
dbutils.widgets.text('catalog', '')
dbutils.widgets.text('schema', '')

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

import pandas as pd
import numpy as np
import requests
from datetime import datetime
import ssl
from google_play_scraper import app, Sort, reviews_all
import json, os, uuid
from app_store_scraper import AppStore
import logging
import time
import certifi

# COMMAND ----------

os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
def get_app_store_rating(app_id, max_retries=5, retry_delay=10):
    retries = 0
    while retries < max_retries:
        try:
            url = f'https://itunes.apple.com/gb/lookup?id={app_id}'
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data and data['results']:
                    return data['results'][0].get('averageUserRating', None)
                else:
                    return None
            else:
                return None
        except Exception as e:
            logging.error(f"Error fetching app store rating: {str(e)}")
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retries += 1
    
    logging.error("Maximum retires exceeded. Failed to fetch app store rating.")
    return None

average_rating_apple = get_app_store_rating('1578902078')

# COMMAND ----------

os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
def get_play_store_rating(app_id, max_retries=5, retry_delay=10):
    retries = 0
    while retries < max_retries:
        try:
            info = app(app_id, lang='en', country='gb')
            average_rating = info.get('score', None)
            return average_rating
        except Exception as e:
            logging.error(f"Error fetching play store rating: {str(e)}")
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retries += 1
    
    logging.error("Maximum retires exceeded. Failed to fetch play store rating.")
    return None 
 
app_id_play_store = 'com.asda.rewards'
average_rating_play_store = get_play_store_rating(app_id_play_store)

# COMMAND ----------

table_name = "key_objectives"

try:
    current_date = datetime.now().date()

    average_rating = (average_rating_apple+average_rating_play_store)/2

    query_1 = f"""
    select count(distinct wallet_id) as count_number
    from {core_catalog}.gb_mb_dl_tables.wallet_pos_txns
    WHERE cast(event_ts as date) between date_add(current_date(), -91) and date_add(current_date(),-1)
    """

    result_1 = spark.sql(query_1).collect()[0]['count_number']

    if catalog == 'custanwo' or catalog == 'custprod':
        query_2 = f"""
        SELECT COUNT(DISTINCT spid) AS count_number
        FROM {catalog}.spv_gold.total_basket
        WHERE timestamp BETWEEN date_add(current_date(), -91) AND current_date()
            AND is_spid_connected IS TRUE
            AND is_active IS TRUE
            AND is_spid_connected_direct IS TRUE
        """
    else:
        result_2 = 8350822 
        
    if 'query_2' in locals():
        result_2 = spark.sql(query_2).collect()[0]['count_number']

    query_3 = f"""
    SELECT sum(earn_value_gbp) as invest_into_cashpot
    FROM {catalog}.{schema}.vw_mssn_wallets_aggregated
    where cmpgn_cmplt_dt >= '2024-01-01'
    """

    result_3 = spark.sql(query_3).collect()[0]['invest_into_cashpot']

    merge_query = f"""
    MERGE INTO {schema}.{table_name} AS target
    using (SELECT '{current_date}' AS date) AS source
    on target.date = source.date
    WHEN MATCHED THEN
        UPDATE SET
            app_rating = {average_rating},
            active_members = {result_1},
            active_connected_cust = {result_2},
            invested_into_cashpot = {result_3}
    WHEN NOT MATCHED THEN
        INSERT (date, app_rating, active_members, active_connected_cust, invested_into_cashpot)
        VALUES ('{current_date}', {average_rating}, {result_1}, {result_2}, {result_3})
    """

    spark.sql(merge_query)

except Exception as e:
    print(f"Error: {str(e)}")
