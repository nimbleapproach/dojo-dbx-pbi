# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "bi_data_model";
# MAGIC CREATE WIDGET TEXT table_name DEFAULT "ghs_shortfall_voucher";

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 


# COMMAND ----------

spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
spark.conf.set ('widget.table_name', dbutils.widgets.get("table_name"))

#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.text('table_name', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS database,
# MAGIC        '${widget.table_name}' AS table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE TABLE IF NOT EXISTS ${widget.catalog}.${widget.schema}.${widget.table_name} (
# MAGIC     Wallet_ID STRING COMMENT "Please add comments.",
# MAGIC     SPID STRING COMMENT "Please add comments.",
# MAGIC     Loyalty_ID STRING COMMENT "Please add comments.", 
# MAGIC     GHS_Order_ID STRING COMMENT "Please add comments.", 
# MAGIC     Delivery_Date DATE COMMENT "Please add comments.", 
# MAGIC     Refund_Date DATE COMMENT "Please add comments.", 
# MAGIC     Rewards_Voucher_Val DOUBLE COMMENT "Please add comments.", 
# MAGIC     Credit_Amt DECIMAL(18,2) COMMENT "Please add comments.", 
# MAGIC     Today_Date DATE COMMENT "Please add comments.", 
# MAGIC     Credit_Indicator INT COMMENT "Please add comments."
# MAGIC      ) 
# MAGIC      COMMENT "This table contains something"
# MAGIC      TBLPROPERTIES ( 
# MAGIC          'delta.minReaderVersion' = '1', 
# MAGIC          'delta.minWriterVersion' = '2'
# MAGIC          );
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.${widget.table_name} LIMIT 10;
# MAGIC

