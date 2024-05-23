# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_time_vouchers_held";
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

# COMMAND ------------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW ${widget.catalog}.${widget.schema}.${widget.view_name}
# MAGIC AS
# MAGIC select * -- create index for powerbi visual axis sorting
# MAGIC     , case when time_held_category = 'Less than 5 minutes' then 1
# MAGIC     when time_held_category = '5-29 minutes' then 2
# MAGIC     when time_held_category = '30 minutes -59 minutes' then 3
# MAGIC     when time_held_category = '1-5 hours' then 4
# MAGIC     when time_held_category = '5-24 hours' then 5
# MAGIC     when time_held_category = '1-2 days' then 6
# MAGIC     when time_held_category = '2-5 days' then 7
# MAGIC     when time_held_category = '5 days +' then 8
# MAGIC     end as time_held_category_order
# MAGIC  from(-- count number of wallets that have held vouchers for each time period
# MAGIC     select count(wallet_id) as num_vouchers
# MAGIC     , case when time_voucher_held_mins < 5 then 'Less than 5 minutes'
# MAGIC         when (time_voucher_held_mins >=5 and time_voucher_held_mins < 30) then '5-29 minutes'
# MAGIC         when (time_voucher_held_mins >=30 and time_voucher_held_mins < 60) then '30 minutes -59 minutes'
# MAGIC         when (time_voucher_held_mins >=60 and time_voucher_held_mins < 300) then '1-5 hours'
# MAGIC         when (time_voucher_held_mins >=300 and time_voucher_held_mins < 1440) then '5-24 hours'
# MAGIC         when (time_voucher_held_mins >=1440 and time_voucher_held_mins < 2880) then '1-2 days'
# MAGIC         when (time_voucher_held_mins >=2880 and time_voucher_held_mins < 7200) then '2-5 days'
# MAGIC         when time_voucher_held_mins >=7200 then '5 days +'
# MAGIC         end as time_held_category
# MAGIC     from
# MAGIC         (-- calculate time a voucher is held in minutes before being redeemed
# MAGIC         select *
# MAGIC         , (timestampdiff(second,reward_gained_ts, reward_redm_ts))*(0.0166667) as time_voucher_held_mins
# MAGIC         from ${widget.catalog}.${widget.schema}.vw_reward_wallets
# MAGIC         where reward_redm_ts is not null)
# MAGIC     group by time_held_category
# MAGIC )
# MAGIC order by time_held_category_order ;
# MAGIC
# MAGIC
# MAGIC
# MAGIC
