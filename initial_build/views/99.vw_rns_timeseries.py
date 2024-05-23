# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_rns_timeseries";
# MAGIC

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
# MAGIC WITH BASE as (
# MAGIC     SELECT 
# MAGIC         sgn.singl_profl_id
# MAGIC         ,sgn.marvel_signup_dt dt
# MAGIC         -- ,wals.mn first_use
# MAGIC         ,coalesce(cast(wals.mn as date), cast('2099-12-31' as date)) first_use
# MAGIC         ,ly.regtn_ts
# MAGIC         ,ly.acct_status_id
# MAGIC     FROM ${widget.catalog}.${widget.schema}.vw_sign_ups sgn
# MAGIC     LEFT OUTER JOIN ${widget.catalog}.${widget.schema}.vw_loyalty_acct ly
# MAGIC         ON sgn.singl_profl_id = ly.singl_profl_id
# MAGIC     LEFT OUTER JOIN 
# MAGIC         (
# MAGIC         SELECT wallet_id, min(cast(event_ts as date)) mn
# MAGIC         FROM ${widget.catalog}.${widget.schema}.vw_wallet_pos_txns
# MAGIC         GROUP BY wallet_id
# MAGIC         ) wals
# MAGIC         ON wals.wallet_id = ly.wallet_id
# MAGIC         order by mn desc
# MAGIC ),
# MAGIC sign_ups
# MAGIC AS
# MAGIC (
# MAGIC   SELECT full_date, count(*) as sign_ups FROM BASE innerx 
# MAGIC   JOIN ${widget.catalog}.${widget.schema}.vw_date_table  main
# MAGIC   ON innerx.dt <= main.full_date
# MAGIC   group by full_date
# MAGIC ),
# MAGIC first_uses
# MAGIC AS
# MAGIC (
# MAGIC   SELECT full_date, count(*) as first_uses FROM BASE innerx 
# MAGIC   JOIN ${widget.catalog}.${widget.schema}.vw_date_table  main
# MAGIC   ON innerx.first_use <= main.full_date
# MAGIC   group by full_date
# MAGIC ),
# MAGIC ct
# MAGIC AS 
# MAGIC (
# MAGIC   SELECT
# MAGIC   main.full_date,
# MAGIC   count(*) as ct
# MAGIC   FROM BASE innerx
# MAGIC   JOIN ${widget.catalog}.${widget.schema}.vw_date_table  main
# MAGIC   ON cast(innerx.regtn_ts as date) <= (cast(main.full_date as timestamp) -  interval 888 hour) 
# MAGIC   AND acct_status_id = 'TEMP'
# MAGIC   GROUP BY full_date
# MAGIC )
# MAGIC select s.full_date, s.sign_ups, f.first_uses, c.ct
# MAGIC FROM sign_ups s    
# MAGIC join first_uses f  
# MAGIC on s.full_date = f.full_date
# MAGIC left join ct c  
# MAGIC on s.full_date = c.full_date ;
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
