# Databricks notebook source
dbutils.widgets.text('catalog', '')
dbutils.widgets.text('schema', '')
dbutils.widgets.text('external_volume_location_name', '')
dbutils.widgets.text('external_volume_location_path', '')


catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
external_volume_location_name = dbutils.widgets.get('external_volume_location_name')
external_volume_location_path = dbutils.widgets.get('external_volume_location_path')


    

# COMMAND ----------

# Print Create external volume command
print(
    f"""
    CREATE EXTERNAL VOLUME {catalog}.{schema}.ext_vol_{schema}
    LOCATION '{external_volume_location_path}/{schema}';

    """
)    

# COMMAND ----------

spark.sql( 
    f"""
    CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.ext_vol_{schema}
    LOCATION '{external_volume_location_path}/{schema}';

    """
)  
