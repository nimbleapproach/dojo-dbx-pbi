# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoring - Get tables row count

# COMMAND ----------

#create widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

#set widget value to be able to use in sql environment
spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))

# COMMAND ----------

#Import packages
from pyspark.sql import functions as sf
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import monotonically_increasing_id,row_number
from pyspark.sql import Window

# COMMAND ----------
  #String to filter by event date:
event_date_filter = ' WHERE cast(event_ts as date) < current_date()'
  #create table list to get counts
prod_catalog_table_list= [
    ('coreprod','gb_customer_data_domain_odl','cdd_odl_cashpot_adjs', ''),
    ('coreprod','gb_customer_data_domain_rpt','cdd_rpt_dim_calendar', ''),
    ('coreprod','gb_customer_data_domain_secured_odl','cdd_odl_singl_profl_customer', ''),
    ('coreprod','gb_customer_data_domain_secured_rpt','cdd_rpt_loyalty_acct', ''),
    ('coreprod','gb_mb_dl_tables','cashpot_bal', event_date_filter),
    ('coreprod','gb_mb_dl_tables','cashpot_setup', event_date_filter),
    ('coreprod','gb_mb_dl_tables','cmpgn_setup', event_date_filter),
    ('coreprod','gb_mb_dl_tables','coupn_wallets', event_date_filter ),
    ('coreprod','gb_mb_dl_tables','cs_credits', event_date_filter ),
    ('coreprod','gb_mb_dl_tables','ext_trans', event_date_filter),
    ('coreprod','gb_mb_dl_tables','game_wallets', event_date_filter ),
    ('coreprod','gb_mb_dl_tables','mssn_wallets',event_date_filter),
    ('coreprod','gb_mb_dl_tables','reward_wallets',event_date_filter),
    ('coreprod','gb_mb_dl_tables','wallet_pos_txns', event_date_filter),
    ('coreprod','gb_mb_secured_dl_tables','loyalty_acct',event_date_filter),
    ('coreprod','gb_mb_secured_dl_tables','ghs_order_kafka','')
]
prod_catalog_table_list_dataframe = spark.createDataFrame(prod_catalog_table_list,["table_catalog","table_schema","table_name", "where_filter"])

# COMMAND ----------


#Prepare sql statement to list all of the tables in the database 
vsql_statement= "SELECT DISTINCT table_catalog,table_schema,table_name FROM information_schema.tables "\
                "WHERE table_catalog='"+dbutils.widgets.get("catalog")+"' and table_schema ='"+dbutils.widgets.get("schema")+"'" 

print('vsql_statement : ' + vsql_statement )

#combine list of tables 
table_list_dataframe=spark.sql(vsql_statement).withColumn("where_filter", lit(''))
table_list_dataframe=table_list_dataframe.union(prod_catalog_table_list_dataframe)
table_list_dataframe = table_list_dataframe.dropDuplicates()

#create table list dataframe and add extra columns
table_list_dataframe=table_list_dataframe.withColumn("sql_query_column", sf.concat(sf.lit('SELECT COUNT(1) AS row_count FROM '),sf.col("table_catalog"),sf.lit('.'),sf.col("table_schema"),sf.lit('.'),sf.col("table_name"),sf.col("where_filter")))
table_list_dataframe=table_list_dataframe.withColumn("row_count", lit(None).cast(IntegerType()))
table_list_dataframe=table_list_dataframe.withColumn("dts", current_timestamp())
table_list_dataframe=table_list_dataframe.withColumn("error_message", lit(None).cast(StringType()))
table_list_dataframe = table_list_dataframe.withColumn('row_id', row_number().over(Window.orderBy('sql_query_column')))

# COMMAND ----------

#loop through each row of the dataframe and capture row counts.
#log error message if the view/table is not able to return counts
for rows in table_list_dataframe.select("row_id", "sql_query_column").collect():
    try:
        row_count=lit(None).cast(IntegerType())
        table_counts_df = spark.sql(rows["sql_query_column"])
        row_count=table_counts_df.first().row_count
        table_list_dataframe=table_list_dataframe.withColumn("row_count",sf.when(sf.col("row_id")==rows["row_id"],row_count).otherwise(sf.col("row_count")))
    except  Exception as e:
        table_list_dataframe=table_list_dataframe.withColumn("error_message",sf.when(sf.col("row_id")==rows["row_id"],str(e)).otherwise(sf.col("error_message")))  

#create temp view of the resultset
table_list_dataframe.createOrReplaceTempView("table_list")
display(table_list_dataframe)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${widget.catalog}.${widget.schema}.monitoring_table_row_count
# MAGIC (
# MAGIC  table_catalog
# MAGIC ,table_schema
# MAGIC ,table_name
# MAGIC ,row_count
# MAGIC ,dts
# MAGIC ,error_message
# MAGIC )
# MAGIC SELECT 
# MAGIC  table_catalog
# MAGIC ,table_schema
# MAGIC ,table_name
# MAGIC ,row_count
# MAGIC ,dts
# MAGIC ,error_message
# MAGIC FROM table_list;

# COMMAND ----------

dbutils.widgets.removeAll()
