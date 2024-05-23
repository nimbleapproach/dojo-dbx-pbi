# Databricks notebook source
# DBTITLE 0,Notebook to populate ghs_shortfall_voucher table
# MAGIC %md
# MAGIC ##Process:     data_quality_dependency_status
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating widgets
# MAGIC ######Input widgets and apply parameters 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";

# COMMAND ----------

spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.core_catalog}' AS core_catalog,
# MAGIC        '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation
# MAGIC ######Query coreprod data into a temporay view  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_data_quality_dependency_status
# MAGIC AS
# MAGIC WITH latest_check_status_cte AS (
# MAGIC     SELECT sd.check_id AS check_id,MAX(sd.dts) AS lastest_dts
# MAGIC     FROM  ${widget.catalog}.${widget.schema}.data_quality_check_status as sd
# MAGIC     GROUP BY sd.check_id
# MAGIC )
# MAGIC , status_cte as (
# MAGIC SELECT dd.dependency_id,
# MAGIC        dd.name AS dependency_name, 
# MAGIC        dd.type AS dependency_type, 
# MAGIC        sd.check_id, 
# MAGIC        sd.definition AS check_definition,
# MAGIC        sd.status, 
# MAGIC        cd.severity,
# MAGIC        sd.dts,  
# MAGIC        CASE WHEN sd.status = false AND cd.severity ='High' THEN 1 ELSE 0 END AS failure
# MAGIC        FROM ${widget.catalog}.${widget.schema}.data_quality_check_status AS sd
# MAGIC JOIN ${widget.catalog}.${widget.schema}.data_quality_dependency_mapping AS cd 
# MAGIC    ON sd.check_id = cd.check_id
# MAGIC JOIN ${widget.catalog}.${widget.schema}.data_quality_dependency_definition AS dd
# MAGIC    ON cd.dependency_id = dd.dependency_id
# MAGIC JOIN latest_check_status_cte AS ld 
# MAGIC    ON sd.check_id = ld.check_id
# MAGIC    AND sd.dts = ld.lastest_dts
# MAGIC  )
# MAGIC SELECT check_id,
# MAGIC        check_definition,
# MAGIC        severity,
# MAGIC        dependency_id,
# MAGIC        dependency_type,
# MAGIC        dependency_name,
# MAGIC        dts,
# MAGIC        status,
# MAGIC        CASE WHEN SUM(failure) OVER(PARTITION BY dependency_name,dependency_type) >0 THEN false ELSE true END AS overall_status 
# MAGIC FROM status_cte 
# MAGIC ORDER BY dependency_type,
# MAGIC          dependency_name,
# MAGIC          check_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Log Status:

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC select *, ROW_NUMBER() OVER (PARTITION BY dependency_id ORDER BY check_id) as row_number from temp_data_quality_dependency_status
# MAGIC )
# MAGIC
# MAGIC INSERT INTO  ${widget.catalog}.${widget.schema}.data_quality_dependency_status_log
# MAGIC                         (dependency_id, dependency_name, dependency_type, overall_status)
# MAGIC               SELECT dependency_id, dependency_name, dependency_type, overall_status FROM cte
# MAGIC               WHERE row_number = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Load
# MAGIC ######Truncate and insert strategy. Truncate table, query temporary view created above then insert into schema table.  

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE  ${widget.catalog}.${widget.schema}.data_quality_dependency_status;
# MAGIC
# MAGIC INSERT INTO TABLE  ${widget.catalog}.${widget.schema}.data_quality_dependency_status
# MAGIC (
# MAGIC        dependency_id,
# MAGIC        dependency_type,
# MAGIC        dependency_name,
# MAGIC        check_id,
# MAGIC        check_definition,
# MAGIC        severity,
# MAGIC        dts,
# MAGIC        status,
# MAGIC        overall_status 
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC        dependency_id,
# MAGIC        dependency_type,
# MAGIC        dependency_name,
# MAGIC        check_id,
# MAGIC        check_definition,
# MAGIC        severity,
# MAGIC        dts,
# MAGIC        status,
# MAGIC        overall_status 
# MAGIC FROM 
# MAGIC     temp_data_quality_dependency_status
# MAGIC ORDER BY  
# MAGIC        dependency_type,
# MAGIC        dependency_name,
# MAGIC        check_id;
# MAGIC
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC ######Count rows from target table  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC count(*) as row_count
# MAGIC FROM ${widget.catalog}.${widget.schema}.data_quality_dependency_status
