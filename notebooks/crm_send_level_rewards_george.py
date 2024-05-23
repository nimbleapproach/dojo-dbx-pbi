# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: CRM Send level George Rewards
# MAGIC ######Daily capture of customer level Test and Control transactions pre and post email send date from salesforce

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

catalog =  dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
core_catalog=dbutils.widgets.get("core_catalog")


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
# MAGIC ##Building Pre and Post Reward George transactional aggregations
# MAGIC ######Pushing results into a temporary view
# MAGIC
# MAGIC ######Pre = 42 days previous and Post = 14 days after the email send date

# COMMAND ----------

tbl_count = spark.table(f"{catalog}.{schema}.crm_send_level_rewards_george").count()
if tbl_count==0:
  df_combinedlogsextract = spark.sql(f"""SELECT eventdate,campaignname,sendtype,walletid 
                                           FROM {core_catalog}.salesforce.combinedlogsextract  
                                           WHERE  cast(eventdate as date) BETWEEN '2023-05-01' AND DATEADD(GETDATE(),-16);""")
else:
  df_combinedlogsextract = spark.sql(f"""SELECT eventdate,campaignname,sendtype,walletid 
                                          FROM {core_catalog}.salesforce.combinedlogsextract  
                                          WHERE cast(eventdate as date) <= DATEADD(GETDATE(),-15) and cast(eventdate as date) >= DATEADD(GETDATE(),-22);""")

df_combinedlogsextract.createOrReplaceTempView("vw_temp_combinedlogsextract")


# COMMAND -----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_tempgeorge
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     'Pre' as type,
# MAGIC     42 as days_analysed,
# MAGIC     journey_id,
# MAGIC     journey_name,
# MAGIC     groups,
# MAGIC     DATE as journey_date,
# MAGIC     count(distinct walletid) as customers,
# MAGIC     count (distinct george_pre_customer) as george_orderers,
# MAGIC     count(george_pre_orders) as  george_orders,
# MAGIC     sum  (george_pre_items)  as  george_items,
# MAGIC     sum  (george_pre_sales)  as  george_sales
# MAGIC  
# MAGIC FROM
# MAGIC  
# MAGIC (
# MAGIC        SELECT
# MAGIC            cast(eventdate as date) as date,
# MAGIC            1234 as journey_id,
# MAGIC            campaignname as journey_name,
# MAGIC            case
# MAGIC            when sendtype = 'Send' then 'Emailed'
# MAGIC            else Sendtype end as groups,
# MAGIC            walletid as walletid
# MAGIC        FROM
# MAGIC            vw_temp_combinedlogsextract 
# MAGIC        WHERE
# MAGIC            --#cast(eventdate as date) = dateadd(GETDATE(),-15)
# MAGIC            campaignname not like '%CashpotExpiry%' and campaignname not like '%Abandoned%' and campaignname not like '%Triggers%'
# MAGIC
# MAGIC
# MAGIC ) T1
# MAGIC  
# MAGIC LEFT JOIN
# MAGIC      
# MAGIC (
# MAGIC SELECT
# MAGIC        lacc.wallet_id                        as george_pre_customer,
# MAGIC        george.creation_date                  as pre_order_date,
# MAGIC        george.sales_order_id                 as george_pre_orders,
# MAGIC        george.sum_order_quantity             as george_pre_items,
# MAGIC        george.total_order_value              as george_pre_sales
# MAGIC FROM
# MAGIC        ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_orders george
# MAGIC        join ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_loyalty_acct lacc
# MAGIC        on george.singl_profl_id = lacc.singl_profl_id
# MAGIC WHERE
# MAGIC        george.creation_date >= dateadd(GETDATE(),-400)
# MAGIC        and george.order_complete = 'Y'
# MAGIC
# MAGIC ) T2
# MAGIC
# MAGIC on T1.walletid = T2.george_pre_customer 
# MAGIC and pre_order_date >= dateadd(T1.date,-42) 
# MAGIC and pre_order_date < cast(T1.date as date)
# MAGIC  
# MAGIC  
# MAGIC GROUP BY
# MAGIC journey_id,journey_name,groups,date
# MAGIC  
# MAGIC UNION ALL
# MAGIC  
# MAGIC  
# MAGIC SELECT
# MAGIC 'Post' as type,
# MAGIC 15 as days_analysed,
# MAGIC journey_id,
# MAGIC journey_name,
# MAGIC groups,
# MAGIC DATE as journey_date,
# MAGIC count(distinct walletid) as email_cust,
# MAGIC count (distinct george_post_customer) as george_orderers2,
# MAGIC count(george_post_orders) as george_orders,
# MAGIC sum  (george_post_items)  as george_items,
# MAGIC sum  (george_post_Sales)  as george_sales
# MAGIC  
# MAGIC FROM
# MAGIC  
# MAGIC (
# MAGIC  SELECT
# MAGIC        cast(eventdate as date) as date,
# MAGIC        1234 as journey_id,
# MAGIC        campaignname as journey_name,
# MAGIC        case
# MAGIC        when sendtype = 'Send' then 'Emailed'
# MAGIC        else Sendtype end as groups,
# MAGIC        walletid as walletid
# MAGIC      FROM
# MAGIC        vw_temp_combinedlogsextract 
# MAGIC      WHERE
# MAGIC         --#cast(eventdate as date) = dateadd(GETDATE(),-15)
# MAGIC         campaignname not like '%CashpotExpiry%' and campaignname not like '%Abandoned%' and campaignname not like '%Triggers%'
# MAGIC
# MAGIC        ) T1
# MAGIC  
# MAGIC LEFT JOIN  
# MAGIC  
# MAGIC (
# MAGIC SELECT        
# MAGIC        lacc.wallet_id                        as george_post_customer,
# MAGIC        george.creation_date                  as post_order_date,
# MAGIC        george.sales_order_id                 as george_post_orders,
# MAGIC        george.sum_order_quantity             as george_post_items,
# MAGIC        george.total_order_value              as george_post_sales
# MAGIC FROM
# MAGIC        ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_orders george
# MAGIC        join ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_loyalty_acct lacc
# MAGIC        on george.singl_profl_id = lacc.singl_profl_id
# MAGIC WHERE
# MAGIC        george.creation_date >= dateadd(GETDATE(),-400)
# MAGIC        and george.order_complete = 'Y'
# MAGIC
# MAGIC ) T3
# MAGIC
# MAGIC on T1.walletid = T3.george_post_customer 
# MAGIC and post_order_date <= dateadd(T1.date,14) 
# MAGIC and post_order_date >= cast(T1.date as date)
# MAGIC  
# MAGIC GROUP BY
# MAGIC journey_id,journey_name,groups,date

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating Temporary view
# MAGIC ######vw_tempgeorge as vw_temprewadsgeorge with reclassified column name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temprewadsgeorge AS
# MAGIC SELECT  
# MAGIC     type, days_analysed, journey_id, journey_name, groups, journey_date, customers,
# MAGIC     george_orderers, george_orders, george_items, george_sales,
# MAGIC     george_orderers/customers AS george_orderers_mailed,
# MAGIC     george_sales/george_orders AS george_avg_order,
# MAGIC     george_orders/george_orderers AS george_orders_orderers,
# MAGIC     george_sales/customers AS george_sales_mailed,
# MAGIC     george_items/george_orders AS george_items_order
# MAGIC FROM 
# MAGIC     vw_tempgeorge
# MAGIC ORDER BY
# MAGIC     1,2,3,4,5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Loading data
# MAGIC ######Merging into table - 'catalog'.bi_data_model.crm_send_level_rewards_george

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC             MERGE INTO ${widget.catalog}.${widget.schema}.crm_send_level_rewards_george AS TARGET
# MAGIC                 USING vw_temprewadsgeorge AS SOURCE
# MAGIC             ON 
# MAGIC                 SOURCE.type = TARGET.type AND
# MAGIC                 SOURCE.journey_name = TARGET.journey_name AND
# MAGIC                 SOURCE.groups = TARGET.groups AND
# MAGIC                 SOURCE.journey_date = TARGET.journey_date
# MAGIC             WHEN MATCHED THEN
# MAGIC                 UPDATE SET *
# MAGIC             WHEN NOT MATCHED THEN
# MAGIC                 INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Removing unwanted historical data
# MAGIC ######Further Logic to be implemented on below delete statement in iteration 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_rewards_george WHERE customers <500

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_rewards_george
# MAGIC WHERE journey_name IN (
# MAGIC     SELECT journey_name
# MAGIC     FROM (
# MAGIC         SELECT distinct journey_date,journey_name, count(distinct groups) as cnt
# MAGIC         FROM ${widget.catalog}.${widget.schema}.crm_send_level_rewards_george
# MAGIC         GROUP BY journey_date,journey_name
# MAGIC     ) t
# MAGIC     WHERE cnt != 2 
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Testing
# MAGIC ######Querying the target table 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(distinct journey_name) FROM ${widget.catalog}.${widget.schema}.crm_send_level_rewards_george

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.crm_send_level_rewards_george
# MAGIC ORDER BY 6 desc,4,2,5
