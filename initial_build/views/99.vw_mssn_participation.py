# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_mssn_participation";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
spark.conf.set ('widget.view_name', dbutils.widgets.get("view_name"))


#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('core_catalog', "")
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.text('view_name', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.core_catalog}' AS core_catalog,
# MAGIC        '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS schema,
# MAGIC        '${widget.view_name}' AS view_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC WITH missions AS (
# MAGIC
# MAGIC     SELECT  
# MAGIC         a.cmpgn_id
# MAGIC         ,SUM(completed_in_last_week) completed_in_last_week
# MAGIC         ,SUM(active_in_last_week) active_in_last_week
# MAGIC         ,(SELECT SUM(YESTERDAY_7_DAY_ACTIVE_FLAG) FROM ${widget.catalog}.${widget.schema}.vw_wallet_activity) total
# MAGIC
# MAGIC     FROM (
# MAGIC         SELECT 
# MAGIC             DISTINCT cmpgn_id, wallet_id
# MAGIC             ,MAX(CASE WHEN cmpgn_cmplt_ts is not null THEN 1 ELSE 0 END) AS completed_in_last_week
# MAGIC             ,max(case when cmpgn_cmplt_ts is null and cast(to_json(mssn_prgrss_struct) as string)!= '{}' then 1 else 0 end) as active_in_last_week
# MAGIC         FROM ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets
# MAGIC         WHERE event_ts between DATE_ADD(CURRENT_DATE(), -8) and DATE_ADD(CURRENT_DATE(), -2) 
# MAGIC         GROUP BY cmpgn_id, wallet_id) a
# MAGIC
# MAGIC         INNER JOIN 
# MAGIC             (SELECT DISTINCT cmpgn_id FROM ${widget.core_catalog}.gb_mb_dl_tables.cmpgn_setup cmpgn
# MAGIC                 WHERE 1=1 AND (cmpgn.cmpgn_tag_array LIKE '%MISSION%'
# MAGIC                 OR cmpgn.cmpgn_tag_array LIKE '%ACCA%')) cmpgn
# MAGIC         ON a.cmpgn_id = cmpgn.cmpgn_id
# MAGIC
# MAGIC     GROUP BY a.cmpgn_id
# MAGIC
# MAGIC     
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     cmpgn_id 
# MAGIC     ,'Mission' AS mssn_type 
# MAGIC     ,'active_perc' AS calc_type 
# MAGIC     ,active_in_last_week dist_cmplt 
# MAGIC     ,total
# MAGIC     ,cast(active_in_last_week as float) / cast(total as float) AS proportion 
# MAGIC FROM missions
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     cmpgn_id 
# MAGIC     ,'Mission' AS mssn_type 
# MAGIC     ,'redemption_perc' AS calc_type 
# MAGIC     ,completed_in_last_week dist_cmplt 
# MAGIC     ,total
# MAGIC     ,cast(completed_in_last_week as float) / cast(total as float) AS proportion 
# MAGIC FROM missions 
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     cmpgn_id
# MAGIC     ,'SP' as mssn_type
# MAGIC     ,'redemption_perc' as calc_type
# MAGIC     ,dist_cmplt
# MAGIC     ,total
# MAGIC     ,cast(dist_cmplt as float) / cast(total as float) as proportion
# MAGIC FROM
# MAGIC
# MAGIC (
# MAGIC     SELECT
# MAGIC         wallets.cmpgn_id
# MAGIC         ,COUNT(DISTINCT wallets.wallet_id) dist_cmplt
# MAGIC         ,(SELECT SUM(YESTERDAY_7_DAY_ACTIVE_FLAG) FROM ${widget.catalog}.${widget.schema}.vw_wallet_activity) total
# MAGIC
# MAGIC     FROM ${widget.core_catalog}.gb_mb_dl_tables.mssn_wallets wallets
# MAGIC     INNER JOIN ${widget.core_catalog}.gb_mb_dl_tables.cmpgn_setup cmpgn
# MAGIC     ON wallets.cmpgn_id = cmpgn.cmpgn_id
# MAGIC     WHERE wallets.event_ts between DATE_ADD(CURRENT_DATE(), -8) and DATE_ADD(CURRENT_DATE(), -2)
# MAGIC     AND (cmpgn.cmpgn_tag_array like '%STARPRODUCT%' OR cmpgn.cmpgn_tag_array like '%OPENSP')
# MAGIC     GROUP BY wallets.cmpgn_id
# MAGIC )
# MAGIC
# MAGIC order by cmpgn_id;
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
