# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: CRM Send level engine
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
# MAGIC ##Building Pre and Post George transactional aggregations
# MAGIC ######Joining combinedlogsextract and cdd_rpt_ft_george_sales_orders into a temporary view
# MAGIC
# MAGIC ######Pre = 42 days previous and Post = 14 days after the email send date

# COMMAND ----------

tbl_count = spark.table(f"{catalog}.{schema}.crm_send_level_engine").count()
if tbl_count==0:
  df_combinedlogsextract = spark.sql(f"""SELECT a.eventdate,a.campaignname,a.sendtype,b.spid 
                                           FROM {core_catalog}.salesforce.combinedlogsextract a JOIN {core_catalog}.salesforce.crmid_spid_delta_mapping b on a.subscriberkey = b.crmid  
                                           WHERE  cast(a.eventdate as date) BETWEEN '2023-05-01' AND DATEADD(GETDATE(),-16);""")
else:
  df_combinedlogsextract = spark.sql(f"""SELECT a.eventdate,a.campaignname,a.sendtype,b.spid 
                                          FROM {core_catalog}.salesforce.combinedlogsextract a JOIN {core_catalog}.salesforce.crmid_spid_delta_mapping b on a.subscriberkey = b.crmid  
                                          WHERE cast(eventdate as date) <= DATEADD(GETDATE(),-15) and cast(eventdate as date) >= DATEADD(GETDATE(),-22);""")

df_combinedlogsextract.createOrReplaceTempView("vw_temp_combinedlogsextract")


# COMMAND ----------

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
# MAGIC     count(distinct spid) as customers,
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
# MAGIC            spid as spid
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
# MAGIC (SELECT distinct
# MAGIC        singl_profl_id                 as george_pre_customer,
# MAGIC        creation_date                  as pre_order_date,
# MAGIC        sales_order_id                 as george_pre_orders,
# MAGIC        sum_order_quantity             as george_pre_items,
# MAGIC        total_order_value              as george_pre_sales
# MAGIC FROM 
# MAGIC        ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_orders
# MAGIC WHERE
# MAGIC        creation_date >= dateadd(GETDATE(),-400)
# MAGIC        --creation_date >= dateadd(day,datediff(day,400,GETDATE()),0)
# MAGIC        and order_complete = 'Y'
# MAGIC
# MAGIC ) T2
# MAGIC
# MAGIC on T1.spid = T2.george_Pre_customer 
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
# MAGIC count(distinct spid) as email_cust,
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
# MAGIC        spid as spid
# MAGIC      FROM
# MAGIC        vw_temp_combinedlogsextract
# MAGIC        WHERE
# MAGIC            --#cast(eventdate as date) = dateadd(GETDATE(),-15)
# MAGIC            campaignname not like '%CashpotExpiry%' and campaignname not like '%Abandoned%' and campaignname not like '%Triggers%'
# MAGIC
# MAGIC        ) T1
# MAGIC  
# MAGIC LEFT JOIN  
# MAGIC  
# MAGIC (SELECT distinct
# MAGIC        singl_profl_id                 as george_post_customer,
# MAGIC        creation_date                  as post_order_date,
# MAGIC        sales_order_id                 as george_post_orders,
# MAGIC        sum_order_quantity             as george_post_items,
# MAGIC        total_order_value              as george_post_sales
# MAGIC FROM 
# MAGIC        ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_orders
# MAGIC WHERE
# MAGIC        creation_date >= dateadd(GETDATE(),-400)
# MAGIC        and order_complete = 'Y'
# MAGIC
# MAGIC ) T3
# MAGIC
# MAGIC on T1.spid = T3.george_post_customer 
# MAGIC and cast(post_order_date as date) <= dateadd(T1.date,14) 
# MAGIC and cast(post_order_date as date) >= cast(T1.date as date)
# MAGIC  
# MAGIC GROUP BY
# MAGIC journey_id,journey_name,groups,date
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Building Pre and Post GHS transactional aggregations
# MAGIC ######Joining combinedlogsextract and ghs_order_kafka into a temporary view
# MAGIC
# MAGIC
# MAGIC ######Pre = 42 days previous and Post = 14 days after the email send date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_tempghs
# MAGIC AS
# MAGIC
# MAGIC SELECT 
# MAGIC 'Pre' as type,
# MAGIC 42 as days_analysed,
# MAGIC journey_id,
# MAGIC journey_name,
# MAGIC groups,
# MAGIC DATE as journey_date,
# MAGIC count ( distinct ghs_pre_customer) as ghs_orderers,
# MAGIC count ( ghs_pre_orders           ) as ghs_orders,
# MAGIC sum   ( ghs_pre_items            ) as ghs_items,
# MAGIC sum   ( ghs_pre_sales            ) as ghs_sales
# MAGIC  
# MAGIC FROM 
# MAGIC
# MAGIC (SELECT
# MAGIC        cast(eventdate as date) as date,
# MAGIC        1234 as journey_id,
# MAGIC        campaignname as journey_name,
# MAGIC        case
# MAGIC        when sendtype = 'Send' then 'Emailed'
# MAGIC        else Sendtype end as groups,
# MAGIC        spid as spid
# MAGIC       FROM
# MAGIC          vw_temp_combinedlogsextract
# MAGIC        WHERE
# MAGIC            --#cast(eventdate as date) = dateadd(GETDATE(),-15)
# MAGIC            campaignname not like '%CashpotExpiry%' and campaignname not like '%Abandoned%' and campaignname not like '%Triggers%'
# MAGIC        ) T1
# MAGIC
# MAGIC LEFT JOIN
# MAGIC
# MAGIC (SELECT 
# MAGIC     distinct a.web_order_id            as ghs_pre_orders,
# MAGIC     a.singl_profl_id                   as ghs_pre_customer,
# MAGIC     cast(a.order_sbmtd_ts as date)     as pre_order_date,
# MAGIC     sum(b.item_qty)                    as ghs_pre_items,
# MAGIC     a.pos_tot_amt                      as ghs_pre_sales
# MAGIC  FROM
# MAGIC     ${widget.core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka a 
# MAGIC     left join ${widget.core_catalog}.gb_mb_dl_tables.ghs_order_item_kafka b on a.web_order_id = b.web_order_id
# MAGIC  WHERE
# MAGIC     a.order_status_nm = 'NO_PENDING_ACTION'
# MAGIC     and cast(a.order_sbmtd_ts as date) >= dateadd(GETDATE(),-400) 
# MAGIC  GROUP BY 
# MAGIC     a.web_order_id,a.singl_profl_id,cast(a.order_sbmtd_ts as date),a.pos_tot_amt ) T2
# MAGIC     on T1.spid = T2.ghs_pre_customer 
# MAGIC     and cast(pre_order_date as date) >= dateadd(T1.date, -42) 
# MAGIC     and cast(pre_order_date as date) < cast(T1.date as date)
# MAGIC  
# MAGIC  GROUP BY 
# MAGIC     journey_id,journey_name,groups,date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC  SELECT 
# MAGIC        'Post' as type,
# MAGIC        15 as days_analysed,
# MAGIC        journey_id,
# MAGIC        journey_name,
# MAGIC        groups,
# MAGIC        date as journey_date,
# MAGIC        count(distinct ghs_post_customer) as ghs_orderers,
# MAGIC        count(ghs_post_orders) as ghs_orders,
# MAGIC        sum( ghs_post_items) as ghs_items,
# MAGIC        sum( ghs_post_sales) as ghs_sales 
# MAGIC  
# MAGIC  FROM 
# MAGIC
# MAGIC (
# MAGIC        SELECT
# MAGIC            cast(eventdate as date) as date,
# MAGIC            1234 as journey_id,
# MAGIC            campaignname as journey_name,
# MAGIC            case
# MAGIC            when sendtype = 'Send' then 'Emailed'
# MAGIC            else Sendtype end as groups,
# MAGIC            spid as spid
# MAGIC        FROM
# MAGIC            vw_temp_combinedlogsextract
# MAGIC        WHERE
# MAGIC            --#cast(eventdate as date) = dateadd(GETDATE(),-15)
# MAGIC            campaignname not like '%CashpotExpiry%' and campaignname not like '%Abandoned%' and campaignname not like '%Triggers%'
# MAGIC        ) T1
# MAGIC         
# MAGIC LEFT JOIN  
# MAGIC (
# MAGIC     SELECT 
# MAGIC        distinct a.web_order_id         as ghs_post_orders,
# MAGIC        a.singl_profl_id                as ghs_post_customer,
# MAGIC        cast(a.order_sbmtd_ts as date)  as post_order_date,
# MAGIC        sum(b.item_qty)                 as ghs_post_items,
# MAGIC        a.pos_tot_amt                   as ghs_post_sales
# MAGIC     FROM
# MAGIC        ${widget.core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka a 
# MAGIC        left join ${widget.core_catalog}.gb_mb_dl_tables.ghs_order_item_kafka b on a.web_order_id = b.web_order_id
# MAGIC     WHERE
# MAGIC        a.order_status_nm = 'NO_PENDING_ACTION'
# MAGIC        and cast(a.order_sbmtd_ts as date) >= dateadd(GETDATE(), -400) 
# MAGIC        group by 
# MAGIC        a.web_order_id,
# MAGIC        a.singl_profl_id,
# MAGIC        cast(a.order_sbmtd_ts as date),
# MAGIC        a.pos_tot_amt
# MAGIC ) T3
# MAGIC on T1.spid = T3.ghs_post_customer 
# MAGIC and cast(post_order_date as date) <= dateadd(T1.date,14) 
# MAGIC and cast(post_order_date as date) >= cast(T1.date as date)
# MAGIC GROUP BY 
# MAGIC journey_id,journey_name,groups,date
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merging George and GHS data
# MAGIC ######Union joining the above two George and GHS temporary views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_campaign_engine
# MAGIC AS
# MAGIC
# MAGIC SELECT 
# MAGIC   DISTINCT grg.type,
# MAGIC   grg.days_analysed, 
# MAGIC   grg.journey_id,
# MAGIC   grg.journey_name,
# MAGIC   grg.groups,
# MAGIC   grg.journey_date,
# MAGIC   george_orderers,
# MAGIC   george_orders,
# MAGIC   george_items, 
# MAGIC   george_sales,
# MAGIC   ghs_orderers, 
# MAGIC   ghs_orders, 
# MAGIC   ghs_items, 
# MAGIC   ghs_sales, 
# MAGIC   customers
# MAGIC
# MAGIC FROM 
# MAGIC   vw_tempgeorge grg 
# MAGIC JOIN 
# MAGIC   vw_tempghs ghs ON grg.journey_id = ghs.journey_id and grg.journey_name = ghs.journey_name and
# MAGIC                   grg.groups = ghs.groups and grg.journey_date = ghs.journey_date and
# MAGIC                   grg.type = ghs.type
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating Temporary view
# MAGIC ######vw_temp_campaign_engine as vw_temp_campaign_engine_final with reclassified column name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_temp_campaign_engine_final AS
# MAGIC SELECT  
# MAGIC     type, days_analysed, journey_id, journey_name, groups, journey_date, customers,
# MAGIC     george_orderers, george_orders, george_items, george_sales,
# MAGIC     ghs_orderers, ghs_orders, ghs_items, ghs_sales,
# MAGIC     ghs_orderers/customers AS ghs_orderers_mailed, 
# MAGIC     george_orderers/customers AS george_orderers_mailed,
# MAGIC     ghs_sales/ghs_orders AS ghs_avg_order,
# MAGIC     george_sales/george_orders AS george_avg_order,
# MAGIC     ghs_orders/ghs_orderers AS ghs_orders_orderers,
# MAGIC     george_orders/george_orderers AS george_orders_orderers,
# MAGIC     ghs_sales/customers AS ghs_sales_mailed,
# MAGIC     george_sales/customers AS george_sales_mailed,
# MAGIC     ghs_items/ghs_orders AS ghs_items_order,
# MAGIC     george_items/george_orders AS george_items_order
# MAGIC FROM 
# MAGIC     vw_temp_campaign_engine
# MAGIC ORDER BY
# MAGIC     1,2,3,4,5

# COMMAND ----------

# MAGIC %md
# MAGIC ##Loading data
# MAGIC ######Merging into table - 'catalog'.bi_data_model.crm_send_level_engine_store

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC             MERGE INTO ${widget.catalog}.${widget.schema}.crm_send_level_engine AS TARGET
# MAGIC                 USING vw_temp_campaign_engine_final AS SOURCE
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
# MAGIC /*DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine AS target
# MAGIC    WHERE EXISTS (SELECT journey_date FROM vw_temp_d2c_campaign_engine AS source WHERE target.journey_date = source.journey_date);
# MAGIC
# MAGIC INSERT INTO ${widget.catalog}.${widget.schema}.crm_send_level_engine
# MAGIC (
# MAGIC     type, 
# MAGIC     days_analysed, 
# MAGIC     journey_id, 
# MAGIC     journey_name, 
# MAGIC     groups, 
# MAGIC     journey_date, 
# MAGIC     customers,
# MAGIC     george_orderers, 
# MAGIC     george_orders, 
# MAGIC     george_items, 
# MAGIC     george_sales,
# MAGIC     ghs_orderers, 
# MAGIC     ghs_orders, 
# MAGIC     ghs_items, 
# MAGIC     ghs_sales,
# MAGIC     ghs_orderers_mailed, 
# MAGIC     george_orderers_mailed,
# MAGIC     ghs_avg_order,
# MAGIC     george_avg_order,
# MAGIC     ghs_orders_orderers,
# MAGIC     george_orders_orderers,
# MAGIC     ghs_sales_mailed,
# MAGIC     george_sales_mailed,
# MAGIC     ghs_items_order,
# MAGIC     george_items_order
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC     type, days_analysed, journey_id, journey_name, groups, journey_date, customers,
# MAGIC     george_orderers, george_orders, george_items, george_sales,
# MAGIC     ghs_orderers, ghs_orders, ghs_items, ghs_sales,
# MAGIC     ghs_orderers/customers AS ghs_orderers_mailed, 
# MAGIC     george_orderers/customers AS george_orderers_mailed,
# MAGIC     ghs_sales/ghs_orders AS ghs_avg_order,
# MAGIC     george_sales/george_orders AS george_avg_order,
# MAGIC     ghs_orders/ghs_orderers AS ghs_orders_orderers,
# MAGIC     george_orders/george_orderers AS george_orders_orderers,
# MAGIC     ghs_sales/customers AS ghs_sales_mailed,
# MAGIC     george_sales/customers AS george_sales_mailed,
# MAGIC     ghs_items/ghs_orders AS ghs_items_order,
# MAGIC     george_items/george_orders AS george_items_order
# MAGIC FROM 
# MAGIC     vw_temp_d2c_campaign_engine
# MAGIC ORDER BY
# MAGIC     1,2,3,4,5*/

# COMMAND ----------

# MAGIC %md
# MAGIC ##Removing unwanted historical data
# MAGIC ######Further Logic to be implemented when source tables change post April 2024

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine WHERE customers <500

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DELETE FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine
# MAGIC WHERE journey_name IN (
# MAGIC     SELECT journey_name
# MAGIC     FROM (
# MAGIC         SELECT distinct journey_date,journey_name, count(distinct groups) as cnt
# MAGIC         FROM ${widget.catalog}.${widget.schema}.crm_send_level_engine
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
# MAGIC SELECT 
# MAGIC     *
# MAGIC FROM 
# MAGIC     ${widget.catalog}.${widget.schema}.crm_send_level_engine
# MAGIC ORDER BY 
# MAGIC     6 desc,4,2,5
