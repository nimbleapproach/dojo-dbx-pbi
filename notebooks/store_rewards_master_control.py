# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: store_rewards_master_control

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

# MAGIC %python
# MAGIC spark.conf.set ('widget.core_catalog', dbutils.widgets.get("core_catalog"))
# MAGIC spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
# MAGIC spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))
# MAGIC
# MAGIC #Reset the widgets values to avoid any caching issues. 
# MAGIC dbutils.widgets.text('core_catalog', "")
# MAGIC dbutils.widgets.text('catalog', "")
# MAGIC dbutils.widgets.text('schema', "")
# MAGIC dbutils.widgets.removeAll()

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_vwRewardsStoreBase
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     T1.control_name as CONTROL_NAME,
# MAGIC     T1.segment as SEGMENT,
# MAGIC     T2.Week as WEEK,
# MAGIC     CASE 
# MAGIC     when T2.Week in (202401,202402,202403,202404) then '1-4'
# MAGIC     when T2.Week in (202405,202406,202407,202408) then '5-8'
# MAGIC     when T2.Week in (202409,202410,202411,202412,202413) then '9-13'
# MAGIC     when T2.Week in (202414,202415,202416,202417) then '14-17'
# MAGIC     when T2.Week in (202418,202419,202420,202421) then '18-21'
# MAGIC     when T2.Week in (202422,202423,202424,202425,202426) then '22-26'
# MAGIC     when T2.Week in (202427,202428,202429,202430) then '27-30'
# MAGIC     when T2.Week in (202431,202432,202433,202434) then '31-34'
# MAGIC     when T2.Week in (202435,202436,202437,202438,202439) then '35-39'
# MAGIC     when T2.Week in (202440,202441,202442,202443) then '40-43'
# MAGIC     when T2.Week in (202444,202445,202446,202447) then '44-47'
# MAGIC     when T2.Week in (202448,202449,202450,202451,202452) then '48-52'
# MAGIC     else '0' end as PERIODs,
# MAGIC     CASE 
# MAGIC     when T2.Week between 202340 and 202352 then 'Pre'when T2.Week > 202352 then 'Post' end as GROUPSs,
# MAGIC     T2.Channel as CHANNEL,
# MAGIC     T1.SPID as SPIDS,
# MAGIC     T1.WALLET_ID as WALLET_ID,
# MAGIC     T2.Visits as VISITS,
# MAGIC     T2.Tot_Sales as SALES,
# MAGIC     T2.Avg_Sales as AVG_SALES_PER_CUSTOMER,
# MAGIC     --ISNULL(T2.Std_Dev,0)  as Std_Dev_PER_CUSTOMER
# MAGIC     --CONVERT(decimal(8,2),T3.Wk_Avg_Sales) as AVG_SALES_AGG_WK,
# MAGIC     --T3.Wk_Std_Dev as Std_Dev_AGG_WK,
# MAGIC     case 
# MAGIC     when T2.Tot_Sales > (T3.Wk_Avg_Sales)-(3*T3.Wk_Std_Dev) or T2.Tot_Sales < (T3.Wk_Avg_Sales)+(3*T3.Wk_Std_Dev)
# MAGIC     then T3.Wk_Avg_Sales else T2.Tot_Sales end as CUST_SPEND_VALUE,
# MAGIC     case 
# MAGIC     when T2.Tot_Sales > (T3.Wk_Avg_Sales)-(3*T3.Wk_Std_Dev) or T2.Tot_Sales < (T3.Wk_Avg_Sales)+(3*T3.Wk_Std_Dev)
# MAGIC     then T3.Wk_Avg_Sales * T2.Visits else T2.Tot_Sales end as NEW_SALES
# MAGIC
# MAGIC --case 
# MAGIC --when CONVERT(decimal(8,2),T2.Tot_Sales) > (CONVERT(decimal(8,2),T2.Avg_Sales)-(3*ISNULL(T2.Std_Dev,0))) or CONVERT(decimal(8,2),T2.Tot_Sales) < (CONVERT(decimal(8,2),T2.Avg_Sales)+(3*ISNULL(T2.Std_Dev,0)))
# MAGIC --then CONVERT(decimal(8,2),T2.Avg_Sales) else CONVERT(decimal(8,2),T2.Tot_Sales) end as Cust_Spend_Value2,
# MAGIC
# MAGIC --case 
# MAGIC --when CONVERT(decimal(8,2),T2.Tot_Sales) > (CONVERT(decimal(8,2),T2.Avg_Sales)-(3*ISNULL(T3.Wk_Std_Dev,0))) or CONVERT(decimal(8,2),T2.Tot_Sales) < (CONVERT(decimal(8,2),T2.Avg_Sales)+(3*ISNULL(T3.Wk_Std_Dev,0)))
# MAGIC --then CONVERT(decimal(8,2),T2.Avg_Sales) else CONVERT(decimal(8,2),T2.Tot_Sales) end as Cust_Spend_Value3
# MAGIC
# MAGIC FROM 
# MAGIC
# MAGIC      (
# MAGIC SELECT 
# MAGIC     a.Control_name as Control_name,
# MAGIC     a.control_group as Segment,
# MAGIC     a.single_proflid as SPID,
# MAGIC     b.wallet_id as Wallet_id
# MAGIC FROM 
# MAGIC     ${widget.catalog}.${widget.schema}.master_control_archive A
# MAGIC INNER JOIN
# MAGIC     ${widget.core_catalog}.gb_mb_secured_dl_tables.loyalty_acct b on a.single_proflid = b.singl_profl_id
# MAGIC WHERE 
# MAGIC     a.Control_name = 'master2024-01-08'
# MAGIC      ) T1
# MAGIC      
# MAGIC      INNER JOIN
# MAGIC           (
# MAGIC           SELECT a.wallet_id as Wallet_id, 
# MAGIC           a.chnl_nm as Channel, 
# MAGIC           c.asda_wk_nbr as Week,
# MAGIC           count(a.trans_nbr) as Visits,
# MAGIC           round(sum(a.sale_amt_inc_vat),2) as Tot_Sales,
# MAGIC           round(AVG(a.sale_amt_inc_vat),2) as Avg_Sales,
# MAGIC           round(STDDEV(a.sale_amt_inc_vat),2) as Std_Dev
# MAGIC           FROM
# MAGIC            ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_wallet_pos_txns a
# MAGIC                INNER JOIN  ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar c on a.visit_dt = c.day_date
# MAGIC             WHERE
# MAGIC             a.chnl_nm = 'store'
# MAGIC             and c.asda_wk_nbr > 202339
# MAGIC             GROUP BY
# MAGIC             a.wallet_id,a.chnl_nm,c.asda_wk_nbr
# MAGIC             ) T2
# MAGIC
# MAGIC ON T1.wallet_id = T2.wallet_id
# MAGIC
# MAGIC INNER JOIN
# MAGIC      (SELECT 
# MAGIC           a.chnl_nm as Channel, 
# MAGIC           c.asda_wk_nbr as Week,
# MAGIC           count(a.wallet_id) as Wallet_Ids,
# MAGIC           count(a.trans_nbr) as Visits,
# MAGIC           round(sum(a.sale_amt_inc_vat),2) as Tot_Sales,
# MAGIC           round(AVG(a.sale_amt_inc_vat),2) as Wk_Avg_Sales,
# MAGIC           round(STDDEV(a.sale_amt_inc_vat),2) as Wk_Std_Dev
# MAGIC           FROM
# MAGIC             ${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_wallet_pos_txns a
# MAGIC                INNER JOIN  ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar c on a.visit_dt = c.day_date
# MAGIC          WHERE
# MAGIC             a.chnl_nm = 'store'
# MAGIC             and c.asda_wk_nbr > 202339
# MAGIC             GROUP BY
# MAGIC             a.chnl_nm,c.asda_wk_nbr) T3
# MAGIC
# MAGIC      ON T2.week = T3.week
# MAGIC
# MAGIC      GROUP BY 
# MAGIC          T1.control_name,
# MAGIC          T1.segment,
# MAGIC          T2.Week,
# MAGIC          CASE when T2.Week between 202340 and 202352 then 'Pre'when T2.Week > 202352 then 'Post' end,
# MAGIC          T2.Channel, 
# MAGIC          T1.SPID,T1.WALLET_ID,
# MAGIC          T2.Visits, T2.Tot_Sales, T2.Avg_Sales, T2.Std_Dev,
# MAGIC          T3.Wk_Avg_Sales, T3.Wk_Std_Dev
# MAGIC
# MAGIC           order by
# MAGIC           2,3,4

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Load
# MAGIC ######Insert strategy. Query temporary view created above then insert into schema table.  

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${widget.catalog}.${widget.schema}.StoreRewards_MasterControl
# MAGIC (
# MAGIC     Control_Group,
# MAGIC     Week_id,
# MAGIC     Periods,
# MAGIC     Analysis_Type,
# MAGIC     segment,
# MAGIC     Channel,
# MAGIC     Orderers,
# MAGIC     Orders,
# MAGIC     NEW_SALES,
# MAGIC     Sales,
# MAGIC     load_ts
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     CONTROL_NAME as Control_Group,
# MAGIC     WEEK as Week_id,
# MAGIC     PERIODs as Periods,
# MAGIC     GROUPSs as Analysis_Type,
# MAGIC     SEGMENT as segment,
# MAGIC     CHANNEL as Channel,
# MAGIC     COUNT(SPIDS) as Orderers,
# MAGIC     SUM(VISITS) AS Orders,
# MAGIC     round(SUM(NEW_SALES),2) AS NEW_SALES,
# MAGIC     round(SUM(SALES),2) AS Sales,
# MAGIC     getdate() as load_ts
# MAGIC FROM 
# MAGIC     temp_vwRewardsStoreBase
# MAGIC WHERE
# MAGIC     --visit_dt >= dateadd(getdate(),-9) and visit_dt <=dateadd(getdate(),-3) 
# MAGIC     week in (
# MAGIC     SELECT CASE WHEN right(asda_wk_nbr,2) = '01' THEN concat(asda_year -1,'52') ELSE asda_wk_nbr-1 END FROM ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar where day_date = TO_DATE(getdate()))
# MAGIC GROUP BY 
# MAGIC     1,2,3,4,5,6
# MAGIC ORDER BY
# MAGIC     2,5,3,4
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test
# MAGIC ######Query table, order by latest week.  
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   *
# MAGIC FROM
# MAGIC   ${widget.catalog}.${widget.schema}.StoreRewards_MasterControl
# MAGIC ORDER BY 
# MAGIC   week_id
