# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_trading_one_off_header";

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
# MAGIC SELECT 'Rewards promotion type, e.g. Super Star Product.' as promotion_type, 
# MAGIC       'Promotion campaign name from the Asda Rewards cmpgn_setup table.' as campaign_name,
# MAGIC       'Promotion campaign ID from the Asda Rewards cmpgn_setup table.' as campaign_id,
# MAGIC       'Official start date of the promotion from the Asda Rewards cmpgn_setup table.' as campaign_start_date,
# MAGIC       'Official end date for the promotion from the Asda Rewards cmpgn_setup table.' as campaign_end_date,
# MAGIC       'Percentage of EPOS items that generated a promotion redemption due to a Rewards bardcode being scanned at the till.' as rewards_participation,
# MAGIC       'Cashpot earn as a percentage of total Asda spend for the campaign during the official campaign period.' as epos_sales_participation,
# MAGIC       'Cashpot earn as a percentage of total Asda Rewards spend for the campaign during the official campaign period.' as giveaway_perc,
# MAGIC       'Total number of promotion redemptions during the whole campaign. All Asda channels and promotion EANs included.' as rewards_redemption_volume,
# MAGIC       'Total amount of rewards cashpot earn generated by promotion redemptions during the whole campaign.' as rewards_cash_pot,
# MAGIC       'Percentage of customers that scanned their Asda Rewards barcode that also purchased the Star Product and redeemed the promotion during the official campaign period.' as rewards_customer_penetration,
# MAGIC       'Total number of items sold by Asda during the whole campaign. All channels and promotion EANs included.' as total_epos_volume
