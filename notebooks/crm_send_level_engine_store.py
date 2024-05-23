# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: CRM Send level engine (Store)
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
# MAGIC ##Building Pre and Post Reward In-Store transactional aggregations
# MAGIC ######Pushing results into a temporary view
# MAGIC
# MAGIC ######Pre = 42 days previous and Post = 14 days after the email send date

# COMMAND ----------

tbl_count = spark.table(f"{catalog}.{schema}.crm_send_level_engine_store").count()
if tbl_count==0:
  df_combinedlogsextract = spark.sql(f"""SELECT eventdate,campaignname,sendtype,walletid 
                                           FROM {core_catalog}.salesforce.combinedlogsextract  
                                           WHERE  cast(eventdate as date) BETWEEN '2023-05-01' AND DATEADD(GETDATE(),-16);""")
else:
  df_combinedlogsextract = spark.sql(f"""SELECT eventdate,campaignname,sendtype,walletid 
                                          FROM {core_catalog}.salesforce.combinedlogsextract  
                                          WHERE cast(eventdate as date) <= DATEADD(GETDATE(),-15) and cast(eventdate as date) >= DATEADD(GETDATE(),-22);""")

df_combinedlogsextract.createOrReplaceTempView("vw_temp_combinedlogsextract")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_tempstore
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
# MAGIC     count (distinct store_pre_customer) as store_orderers,
# MAGIC     count(store_pre_orders) as  store_orders,
# MAGIC     sum  (store_pre_items)  as  store_items,
# MAGIC     sum  (store_pre_sales)  as  store_sales
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
# MAGIC wpt.wallet_id          as store_pre_customer,
# MAGIC a.visit_dt             as pre_order_date,
# MAGIC a.basket_id            as store_pre_orders,
# MAGIC a.item_qty             as store_pre_items,
# MAGIC a.sale_amt_inc_vat     as store_pre_sales
# MAGIC FROM ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_wallet_pos_txns wpt
# MAGIC      inner join ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_transaction a on wpt.basket_id = a.basket_id
# MAGIC WHERE 
# MAGIC wpt.chnl_nm = 'store'
# MAGIC and a.channel_id != 2
# MAGIC and a.visit_dt >= dateadd(GETDATE(),-365)
# MAGIC
# MAGIC ) T2
# MAGIC
# MAGIC on T1.walletid = T2.store_Pre_customer 
# MAGIC and cast(pre_order_date as date) >= dateadd(T1.date,-42) 
# MAGIC and cast(pre_order_date as date) < cast(T1.date as date)
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
# MAGIC count (distinct store_post_customer) as store_orderers2,
# MAGIC count(store_post_orders) as store_orders,
# MAGIC sum  (store_post_items)  as store_items,
# MAGIC sum  (store_post_Sales)  as store_sales
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
# MAGIC         SELECT 
# MAGIC         wpt.wallet_id          as store_post_customer,
# MAGIC         a.visit_dt             as post_order_date,
# MAGIC         a.basket_id            as store_post_orders,
# MAGIC         a.item_qty             as store_post_items,
# MAGIC         a.sale_amt_inc_vat     as store_post_sales
# MAGIC         FROM ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_wallet_pos_txns wpt
# MAGIC         inner join ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_transaction a on wpt.basket_id = a.basket_id
# MAGIC         WHERE 
# MAGIC         wpt.chnl_nm = 'store'
# MAGIC         and a.channel_id != 2
# MAGIC         and a.visit_dt >= dateadd(GETDATE(),-365)
# MAGIC
# MAGIC ) T3
# MAGIC
# MAGIC on T1.walletid = T3.store_post_customer 
# MAGIC and cast(post_order_date as date) <= dateadd(T1.date,14) 
# MAGIC and cast(post_order_date as date) >= cast(T1.date as date)
# MAGIC  
# MAGIC GROUP BY
# MAGIC journey_id,journey_name,groups,date

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating Temporary view
# MAGIC ######vw_tempstore as vw_tempstore1 with reclassified column name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_tempstore1 AS
# MAGIC SELECT  
# MAGIC     type, days_analysed, journey_id, journey_name, groups, journey_date, customers,
# MAGIC     store_orderers, store_orders, store_items, store_sales,
# MAGIC     store_orderers/customers AS store_orderers_mailed,
# MAGIC     store_sales/store_orders AS store_avg_order,
# MAGIC     store_orders/store_orderers AS store_orders_orderers,
# MAGIC     store_sales/customers AS store_sales_mailed,
# MAGIC     store_items/store_orders AS store_items_order
# MAGIC FROM 
# MAGIC     vw_tempstore
# MAGIC ORDER BY
# MAGIC     1,2,3,4,5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Loading data
# MAGIC ######Merging into table - 'catalog'.bi_data_model.crm_send_level_engine_store

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC             MERGE INTO ${widget.catalog}.${widget.schema}.crm_send_level_engine_store AS TARGET
# MAGIC                 USING vw_tempstore1 AS SOURCE
# MAGIC             ON 
# MAGIC                 SOURCE.type = TARGET.type AND
# MAGIC                 SOURCE.journey_name = TARGET.journey_name AND
# MAGIC                 SOURCE.groups = TARGET.groups AND
# MAGIC                 SOURCE.journey_date = TARGET.journey_date
# MAGIC             WHEN MATCHED THEN
# MAGIC                 UPDATE SET *
# MAGIC             WHEN NOT MATCHED THEN
# MAGIC                 INSERT *;
# MAGIC
# MAGIC
# MAGIC /*DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine_store AS target
# MAGIC    WHERE EXISTS (SELECT journey_date FROM vw_tempstore AS source WHERE target.journey_date = source.journey_date);
# MAGIC
# MAGIC INSERT INTO ${widget.catalog}.${widget.schema}.crm_send_level_engine_store
# MAGIC (
# MAGIC     type, 
# MAGIC     days_analysed, 
# MAGIC     journey_id, 
# MAGIC     journey_name, 
# MAGIC     groups, 
# MAGIC     journey_date, 
# MAGIC     customers,
# MAGIC     store_orderers, 
# MAGIC     store_orders, 
# MAGIC     store_items, 
# MAGIC     store_sales,
# MAGIC     store_orderers_mailed, 
# MAGIC     store_avg_order,
# MAGIC     store_orders_orderers,
# MAGIC     store_sales_mailed,
# MAGIC     store_items_order
# MAGIC
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     type, days_analysed, journey_id, journey_name, groups, journey_date, customers,
# MAGIC     store_orderers, store_orders, store_items, store_sales,
# MAGIC     store_orderers/customers AS store_orderers_mailed,
# MAGIC     store_sales/store_orders AS store_avg_order,
# MAGIC     store_orders/store_orderers AS store_orders_orderers,
# MAGIC     store_sales/customers AS store_sales_mailed,
# MAGIC     store_items/store_orders AS store_items_order
# MAGIC FROM 
# MAGIC     vw_tempstore
# MAGIC ORDER BY
# MAGIC     1,2,3,4,5
# MAGIC     */

# COMMAND ----------

# MAGIC %md
# MAGIC ##Removing unwanted historical data
# MAGIC ######Further Logic to be implemented on below delete statement in iteration 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine_store WHERE customers <500

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine_store
# MAGIC WHERE journey_name IN (
# MAGIC     SELECT journey_name
# MAGIC     FROM (
# MAGIC         SELECT distinct journey_date,journey_name, count(distinct groups) as cnt
# MAGIC         FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine_store
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
# MAGIC SELECT COUNT(distinct journey_name) FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine_store

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine_store
# MAGIC ORDER BY 6 desc,4,2,5
