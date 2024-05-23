# Databricks notebook source
# MAGIC %md
# MAGIC ##Process: crm_monthly_ghs
# MAGIC ######Weekly insert of overall crm customer email performance test v control using the the latest 'master control' population

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
# MAGIC ###Assign a cutoff week for delta load

# COMMAND ----------

cutoff_wk = spark.sql(
        """select asda_wk_nbr-2 from ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar where day_date = to_date(getdate())"""
)
wk = cutoff_wk.first()[0]
print(wk)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Get the row count of crm_monthly_ghs table

# COMMAND ----------

crm_monthly_ghs = spark.table("""${widget.catalog}.${widget.schema}.crm_monthly_ghs""")
tbl_cnt = crm_monthly_ghs.count()
display(tbl_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Condition based data load
# MAGIC ######If table has no records, then load the entire history else merge data from cutoff week onwards

# COMMAND ----------

if (tbl_cnt == 0):
    print("Full load as the table is empty")
    spark.conf.set("widget.wk_nbr", 202416)
else:
    print("Data load post cut off week")
    spark.conf.set("widget.wk_nbr", str(wk))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Building Pre and Post ghs transactional aggregations
# MAGIC ######Joining 'master control' population and gb_mb_secured_dl_tables.ghs_order_kafka into a temporary view
# MAGIC
# MAGIC ######Pre = 13 weeks prior to control group creation date and Post = ongoing until new master control population has been created (appox every 6 months)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_crm_ghs
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC     T1.control_name as control_group,
# MAGIC     T1.segment as segment,
# MAGIC     T2.week as week_id,
# MAGIC     case 
# MAGIC     when T2.Week in (202401,202402,202403,202404,202501,202502,202503,202504,202601,202602,202603,202604) then '1-4'
# MAGIC     when T2.Week in (202405,202406,202407,202408,202505,202506,202507,202508,202605,202606,202607,202608) then '5-8'
# MAGIC     when T2.Week in (202409,202410,202411,202412,202413,202509,202510,202511,202512,202513,202609,202610,202611,202612,202613) then '9-13'
# MAGIC     when T2.Week in (202414,202415,202416,202417,202514,202515,202516,202517,202614,202615,202616,202617) then '14-17'
# MAGIC     when T2.Week in (202418,202419,202420,202421,202518,202519,202520,202521,202618,202619,202620,202621)  then '18-21'
# MAGIC     when T2.Week in (202422,202423,202424,202425,202426,202522,202523,202524,202525,202526,202622,202623,202624,202625,202626) then '22-26'
# MAGIC     when T2.Week in (202427,202428,202429,202430,202527,202528,202529,202530,202627,202628,202629,202630) then '27-30'
# MAGIC     when T2.Week in (202431,202432,202433,202434,202531,202532,202533,202534,202631,202632,202633,202634) then '31-34'
# MAGIC     when T2.Week in (202435,202436,202437,202438,202439,202535,202536,202537,202538,202539,202635,202636,202637,202638,202639) then '35-39'
# MAGIC     when T2.Week in (202440,202441,202442,202443,202540,202541,202542,202543,202640,202641,202642,202643) then '40-43'
# MAGIC     when T2.Week in (202444,202445,202446,202447,202544,202545,202546,202547,202644,202645,202646,202647) then '44-47'
# MAGIC     when T2.Week in (202448,202449,202450,202451,202452,202548,202549,202550,202551,202552,202648,202649,202650,202651,202652) then '48-52'
# MAGIC     else '0' end as periods,
# MAGIC     "ghs" as channel,
# MAGIC     CASE 
# MAGIC     when T2.Week between 202340 and 202352 then 'Pre'when T2.Week > 202352 then 'Post' end as analysis_type,
# MAGIC     count(distinct T2.ghs_customers) as orderers,
# MAGIC     count(T2.ghs_orders) as orders,
# MAGIC     sum(T2.ghs_sales) as sales
# MAGIC
# MAGIC FROM 
# MAGIC
# MAGIC      (  SELECT 
# MAGIC             concat(control_type,file_uploaded_date) as control_name,
# MAGIC             control_group as segment,
# MAGIC             single_proflid as spid
# MAGIC         FROM 
# MAGIC             ${widget.core_catalog}.gb_mb_secured_aggregate_dl_tables.d2c_control_group_archive
# MAGIC         WHERE 
# MAGIC             file_uploaded_date = '2024-01-04'
# MAGIC             and control_type = 'master'
# MAGIC      )  T1
# MAGIC      
# MAGIC  INNER JOIN
# MAGIC
# MAGIC     (   SELECT 
# MAGIC             distinct(a.web_order_id) as ghs_orders,
# MAGIC             b.asda_wk_nbr            as week,
# MAGIC             a.singl_profl_id         as ghs_customers,
# MAGIC             --a.order_sub_tot_amt      as ghs_sales
# MAGIC             a.pos_tot_amt            as ghs_sales
# MAGIC         FROM
# MAGIC             ${widget.core_catalog}.gb_mb_secured_dl_tables.ghs_order_kafka a
# MAGIC             --inner join  ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar b on cast(a.order_sbmtd_ts as date) = b.day_date
# MAGIC             inner join  ${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar b on a.src_create_dt_part = b.day_date
# MAGIC         WHERE
# MAGIC             a.order_status_nm NOT IN ('AMMENDED', 'CANCELLED', 'INCOMPLETE', 'REJECTED', 'NOT_FULFILLED_SETTLEMENT_EXCEPTION', 'NOT_SETTLED_SETTLEMENT_EXCEPTION', 'TEST_SUBMITTED', 'COPIED') and a.fulfmt_type_cd NOT IN ('DP')
# MAGIC             --a.order_status_nm = 'NO_PENDING_ACTION' and a.fulfmt_type_cd NOT IN ('DP')
# MAGIC             and b.asda_wk_nbr > "${widget.wk_nbr}" 
# MAGIC     )  T2
# MAGIC
# MAGIC ON T1.spid = T2.ghs_customers
# MAGIC
# MAGIC  GROUP BY 
# MAGIC          T1.control_name,
# MAGIC          T1.segment,
# MAGIC          T2.week,
# MAGIC          case 
# MAGIC     when T2.Week in (202401,202402,202403,202404,202501,202502,202503,202504,202601,202602,202603,202604) then '1-4'
# MAGIC     when T2.Week in (202405,202406,202407,202408,202505,202506,202507,202508,202605,202606,202607,202608) then '5-8'
# MAGIC     when T2.Week in (202409,202410,202411,202412,202413,202509,202510,202511,202512,202513,202609,202610,202611,202612,202613) then '9-13'
# MAGIC     when T2.Week in (202414,202415,202416,202417,202514,202515,202516,202517,202614,202615,202616,202617) then '14-17'
# MAGIC     when T2.Week in (202418,202419,202420,202421,202518,202519,202520,202521,202618,202619,202620,202621)  then '18-21'
# MAGIC     when T2.Week in (202422,202423,202424,202425,202426,202522,202523,202524,202525,202526,202622,202623,202624,202625,202626) then '22-26'
# MAGIC     when T2.Week in (202427,202428,202429,202430,202527,202528,202529,202530,202627,202628,202629,202630) then '27-30'
# MAGIC     when T2.Week in (202431,202432,202433,202434,202531,202532,202533,202534,202631,202632,202633,202634) then '31-34'
# MAGIC     when T2.Week in (202435,202436,202437,202438,202439,202535,202536,202537,202538,202539,202635,202636,202637,202638,202639) then '35-39'
# MAGIC     when T2.Week in (202440,202441,202442,202443,202540,202541,202542,202543,202640,202641,202642,202643) then '40-43'
# MAGIC     when T2.Week in (202444,202445,202446,202447,202544,202545,202546,202547,202644,202645,202646,202647) then '44-47'
# MAGIC     when T2.Week in (202448,202449,202450,202451,202452,202548,202549,202550,202551,202552,202648,202649,202650,202651,202652) then '48-52'
# MAGIC     else '0' end,
# MAGIC          case 
# MAGIC     when T2.week between 202340 and 202352 then 'Pre'when T2.week > 202352 then 'Post' end
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Load
# MAGIC ######Merge strategy. Query temporary view created above then insert into schema table.  

# COMMAND ----------

# MAGIC %sql
# MAGIC             MERGE INTO ${widget.catalog}.${widget.schema}.crm_monthly_ghs AS TARGET
# MAGIC                 USING vw_crm_ghs  AS SOURCE
# MAGIC             ON 
# MAGIC                 SOURCE.control_group = TARGET.control_group AND
# MAGIC                 SOURCE.week_id = TARGET.week_id AND
# MAGIC                 SOURCE.segment = TARGET.segment 
# MAGIC             WHEN MATCHED THEN
# MAGIC                 UPDATE SET *
# MAGIC             WHEN NOT MATCHED THEN
# MAGIC                 INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*INSERT INTO ${widget.catalog}.${widget.schema}.crm_monthly_ghs
# MAGIC (
# MAGIC     control_group,
# MAGIC     segment,
# MAGIC     week_id,
# MAGIC     periods,
# MAGIC     analysis_type,
# MAGIC     channel,
# MAGIC     orderers,
# MAGIC     orders,
# MAGIC     sales,
# MAGIC     load_ts
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     control_group,
# MAGIC     segment,
# MAGIC     week_id,
# MAGIC     periods,
# MAGIC     analysis_type,
# MAGIC     channel,
# MAGIC     orderers,
# MAGIC     orders,
# MAGIC     sales,
# MAGIC     getdate() as load_ts
# MAGIC FROM 
# MAGIC     vw_crm_ghs  
# MAGIC ORDER BY 
# MAGIC     week_id,periods,segment,analysis_type*/
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
# MAGIC   ${widget.catalog}.${widget.schema}.crm_monthly_ghs
# MAGIC ORDER BY 
# MAGIC   week_id,segment
