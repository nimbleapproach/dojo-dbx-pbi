# Databricks notebook source
dbutils.widgets.text('catalog', '')
dbutils.widgets.text('schema', '')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
    """
)

if catalog != 'custprod':
    spark.sql(
        f"""
        GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema} TO foundations;
        """
    )   

if catalog == 'custanwo':
    spark.sql(
        f"""
        ALTER SCHEMA {catalog}.{schema} OWNER TO `foundations`;
        """
    )
    


    
    
    


# COMMAND ----------

#create ddl deployment table
spark.sql(
    f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ddl_deployment ( 
            id BIGINT GENERATED ALWAYS AS IDENTITY, 
            object STRING NOT NULL COMMENT 'full path to table, view etc', 
            type STRING DEFAULT NULL COMMENT 'table, view, function or other', 
            deployment_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'time when we deployed ddl', 
            deployment_content STRING COMMENT 'full content which are deplyed. This will determine if there is a change',
            test_timestamp TIMESTAMP DEFAULT NULL COMMENT 'after deployment we check version of object and type of object', 
            CONSTRAINT `ddl_deployment_pk` PRIMARY KEY (`object`,`type`,`deployment_timestamp`,`deployment_content`)) 
            USING delta COMMENT 'The ddl_deployment table stores information about the deployment of database objects such as tables, views, and functions. It includes the full path to the object, its type, and the version number. The deployment_timestamp column indicates when the object was deployed, while the test_timestamp column indicates when it was last tested after deployment. This table is useful for tracking changes to database objects and ensuring that they are properly deployed and tested.' 
            TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name', 
                'delta.enableChangeDataFeed' = 'true', 
                'delta.enableDeletionVectors' = 'true', 
                'delta.feature.allowColumnDefaults' = 'supported', 
                'delta.feature.changeDataFeed' = 'supported', 
                'delta.feature.columnMapping' = 'supported', 
                'delta.feature.deletionVectors' = 'supported',
                'delta.feature.invariants' = 'supported', 
                'delta.minReaderVersion' = '3', 
                'delta.minWriterVersion' = '7'
        )
     """
)
