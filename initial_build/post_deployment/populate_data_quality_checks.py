# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Databricks has a bug clearing widgets values on a change. 
# MAGIC Following command is copying wdgets values into the local variable and removing widgets. 
# MAGIC

# COMMAND ----------

spark.conf.set ('widget.catalog', dbutils.widgets.get("catalog"))
spark.conf.set ('widget.schema', dbutils.widgets.get("schema"))

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")


#set workspace_suffix
if catalog == 'custanwo':
    #dev
    workspace_suffix = " [Dev]"
    workflow_suffix = "-dev"
    spark.conf.set ('widget.workspace_suffix', workspace_suffix)
    spark.conf.set ('widget.workflow_suffix', workflow_suffix)
elif catalog == 'custstag':
    #staging
    workspace_suffix = " [Stage]"
    workflow_suffix = '-stage'
    spark.conf.set ('widget.workspace_suffix', workspace_suffix)
    spark.conf.set ('widget.workflow_suffix', workflow_suffix)
elif catalog == 'custprod':
    #prod
    workspace_suffix = ""
    workflow_suffix = '-prod'
    spark.conf.set ('widget.workspace_suffix', workspace_suffix)
    spark.conf.set ('widget.workflow_suffix', workflow_suffix)
else:
    raise Exception("Unexpected catalog: {0}".format(catalog))

#Reset the widgets values to avoid any caching issues. 
dbutils.widgets.text('catalog', "")
dbutils.widgets.text('schema', "")
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${widget.catalog}' AS catalog,
# MAGIC        '${widget.schema}' AS database,
# MAGIC        '${widget.workspace_suffix}' as worksapce_suffix

# COMMAND ----------

# MAGIC %md
# MAGIC ###Copy widgets into local variables
# MAGIC Following command is adding check definition

# COMMAND ----------

# MAGIC %sql
# MAGIC  WITH check_definition_cte AS
# MAGIC  (
# MAGIC           SELECT 1 AS check_id,'The "coreprod.gb_mb_dl_tables.mssn_wallets" table is missing either historical data, contains duplicate data, or is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 2 AS check_id,'The blue light data in "coreprod.gb_mb_dl_tables.mssn_wallets" table is missing either historical data,contains duplicate data, or is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 3 AS check_id,'The "coreprod.gb_mb_dl_tables.wallet_pos_txns" table is either missing historical data, missing data for the current days data load, or contains duplicate data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 4 AS check_id,'The GHS transactions in "coreprod.gb_mb_dl_tables.wallet_pos_txns" table are either missing historical data, missing data for the current days data load, or contains duplicate data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 5 AS check_id,'The "coreprod.gb_mb_dl_tables.cs_credits" table is missing either historical data, or data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 6 AS check_id,'The "coreprod.gb_mb_dl_tables.ext_trans" table is missing either historical data, or data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 7 AS check_id,'The number of rows added to "coreprod.gb_mb_dl_tables.wallet_pos_txns", with chnl_nm of "pfs" (Petrol Filling Station) should not be less than 75% of the median row count over the previous 7 days.' AS definition, True AS active UNION --pfs
# MAGIC           SELECT 8 AS check_id,'Giveaway % in Earn On Fuel Visit Level Table should always be between 0.42 to 0.51 and should not exceed this value.' AS definition, True AS active UNION --pfs
# MAGIC           SELECT 9 AS check_id,'The "coreprod.gb_mb_dl_tables.reward_wallets" table contains duplicate data, is missing historical data, or is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 10 AS check_id,'The "coreprod.gb_mb_secured_dl_tables.loyalty_acct" table is missing data for the current days data load, or contains duplicate data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 11 AS check_id,'The "coreprod.gb_mb_dl_tables.cmpgn_setup" table is missing historic data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 12 AS check_id,'WARNING: The "coreprod.gb_mb_dl_tables.game_wallets" table may be missing historic data if this test fails.' AS definition, True AS active UNION -- set to low everywhere as data is too irregular for this test
# MAGIC           SELECT 13 AS check_id,'The "coreprod.gb_mb_dl_tables.coupn_wallets" table is missing historic data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 14 AS check_id,'The "bi_data_model.loyalty_acct_archive" table is missing historic data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 15 AS check_id,'The "coreprod.gb_customer_data_domain_secured_rpt.cdd_rpt_loyalty_acct" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 16 AS check_id,'The "coreprod.gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 17 AS check_id,'The "coreprod.gb_mb_dl_tables.ghs_order_promo" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 18 AS check_id,'The "bi_data_model.activity_time_series" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 19 AS check_id,'The "coreprod.gb_mb_dl_tables.groc_order_rfnd" table is missing either historical data, or data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 20 AS check_id,'The "bi_data_model.nursery_time_series" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 21 AS check_id,'The "bi_data_model.active_earners" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 22 AS check_id,'The "bi_data_model.app_usage_api" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 23 AS check_id,'The "coreprod.gb_mb_secured_dl_tables.ghs_order_kafka" is either missing data for the current days data load, which corresponds to orders for today-2 days, for historical data or it contains duplicate data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 24 AS check_id,'Warning: The "coreprod.gb_mb_dl_tables.mssn_wallets" table" may be missing data for todays data load.' AS definition, True AS active UNION
# MAGIC           SELECT 25 AS check_id,'The job "foundations_v1_summary_activity_notebooks" has not run today if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 26 AS check_id,'The job "foundations_v1_google_api" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 27 AS check_id,'The job "foundations_v1_ghs_voucher_shortfall" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 28 AS check_id,'The table "gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_orders" is missing data for todays data load if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 29 AS check_id,'The table "gb_mb_dl_tables.ghs_order_item_kafka" has no data if this fails.' AS definition, True as active UNION
# MAGIC           SELECT 30 AS check_id,'The job "foundations_v1_crm_send_level_engine" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 31 AS check_id,'The job "foundations_v1_earn_on_fuel" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 32 AS check_id,'The "coreprod.gb_customer_data_domain_odl.cdd_odl_pfs_sales" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 33 AS check_id,'The "coreprod.gb_mb_store_dl_tables.store_visit" table is missing data for the current days data load if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 34 AS check_id,'The "coreprod.gb_customer_data_domain_rpt.cdd_rpt_lylty_supp_bill" table is missing data for the current days data load if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 35 AS check_id,'The "coreprod.gb_mb_secured_dl_tables.ghs_order_kafka" is either missing data for the current days data load, for historical data or it contains duplicate data if this test fails.' AS definition, True AS active UNION
# MAGIC           SELECT 36 AS check_id,'The job "foundations_v1_ghs_participation" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 37 AS check_id,'The "gb_customer_data_domain_odl.cdd_odl_wallet_pos_txns" table is missing data for the current days data load, missing historical data, or contains duplicate data if this test fails' as definition, True as active UNION
# MAGIC           SELECT 38 AS check_id,'The table "cdd_odl_dim_item_hierarchy" has not been loaded yet today if this check fails.' as definition, True as active UNION
# MAGIC           SELECT 39 AS check_id,'The table "cdd_rpt_transaction_item" has not been loaded yet today if this check fails.' as definition, True as active UNION
# MAGIC           SELECT 40 AS check_id,'The table "gb_customer_data_domain_odl.cdd_odl_cmpgn_setup" has not been loaded yet today if this check fails.' as definition, True as active UNION
# MAGIC           SELECT 41 AS check_id,'The table "gb_customer_data_domain_odl.cdd_odl_cmpgn_products" has not been loaded yet today if this test fails.' as definition, True as active UNION
# MAGIC           SELECT 42 AS check_id,'The job "foundations_v1_crm_send_level_engine_store" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 43 AS check_id, 'The job "cdna_ar" has not run today if this test fails. This populates tables for datamart reports' AS definition, True as active UNION
# MAGIC           SELECT 44 AS check_id, 'The most recent data in the table "cdna_ar.ar_cust_rewards_visits" is not from two days ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 45 AS check_id, 'The most recent data in the table "cdna_ar.ar_cust_full_visit_history" is not from from two days ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 46 AS check_id, 'The most recent data in the table "cdna_ar.ar_cust_full_visit_history", with chnl_nm "ecom", is not from two days ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 47 AS check_id, 'The most recent data in the table "cdna_ar.dsa_ar_customer_smry" is not from last week or more recently if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 48 AS check_id, 'The table "cdna_ar.ar_cust_rewards_burn" does not contain any data from the previous friday or more recently if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 49 AS check_id, 'The table "cdna_ar.dsa_helper_last_wk" does not exist or contain any data if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 50 AS check_id, 'The table "cdna_ar.dsa_helper_week_finder" does not contain any data from last week or more recently if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 51 AS check_id, 'The table "cdna_ar.ar_cust_earning" does not exist or has no data if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 52 AS check_id, 'The table "cdna_ar.v_stg_01_cust_attrib_extended" does not exist or has no data if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 53 AS check_id, 'The table "cdna_ar.ar_cmpgn_setup" does not contain any data from the previous friday or more recently if this test fails.' AS definition, True as active UNION 
# MAGIC           SELECT 54 AS check_id, 'The most recent week in table "cdna_ar.dsa_customer_wk_smry" is not the last complete week if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 55 AS check_id, 'The most recent week in table "cdna_ar.dsa_ar_gsm_wallets" is not the last complete week if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 56 AS check_id, 'The most recent week in table "cdna_ar.dsa_pbi_ar_gsm_store_agg" is not the last complete week if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 57 AS check_id, 'The most recent week in table "cdna_ar.dsa_pbi_ar_bis_case_smry" is not the last complete week if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 58 AS check_id, 'The most recent data in the table "cdna_ar.ar_store_visits_ar_flag" is not from one day ago or two days ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 59 AS check_id, 'The most recent data in the table "cdna_ar.dsa_pbi_ar_sdb_tracker_inc_ghs" is not from one day ago or two days ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 60 AS check_id, 'The most recent data in the table "cdna_ar.dsa_pbi_ar_sdb_date_hierarchy" is not from one day or two days ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 61 AS check_id, 'The most recent data in the table "cdna_ar.dsa_pbi_ar_store_sales_output" is not from one day ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 62 AS check_id, 'The most recent data in the table "cdna_ar.ar_cust_earning" is not from two day ago if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 63 AS check_id, 'The job "cdna_ar_daily_store" has not run today if this test fails. This populates tables for datamart reports' AS definition, True as active UNION
# MAGIC           SELECT 64 AS check_id,'The job "foundations_v1_crm_monthly_ghs" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 65 AS check_id,'The job "foundations_v1_crm_monthly_george" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 66 AS check_id,'The table "gb_mb_dl_tables.wallet_pos_txns" is missing data for some individual stores if this test fails' AS definition, True as active UNION
# MAGIC           SELECT 67 AS check_id,'The table "gb_wm_mb_vm.visit" is missing data for the current days data load if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 68 AS check_id,'The job "foundations_v1_key_objectives" has not run today if this test fails.' AS definition, True as active UNION
# MAGIC           SELECT 69 AS check_id,'The table "gb_customer_data_domain_odl.cdd_odl_loyalty_acct" is missing data for todays data load if this test fails.' AS definition, True as active
# MAGIC  )
# MAGIC
# MAGIC MERGE INTO ${widget.catalog}.${widget.schema}.data_quality_check_definition AS target 
# MAGIC USING check_definition_cte AS source
# MAGIC ON source.check_id = target.check_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE;
# MAGIC     
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.data_quality_check_definition LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Following command is adding check depenencies

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH check_dependency_definition_cte AS 
# MAGIC (
# MAGIC   SELECT 1 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Reporting Earn and Burn' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Reporting Earn and Burn","schedule type":"Daily"}' AS parameter 
# MAGIC   UNION
# MAGIC   SELECT 2 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Reporting Blue Light' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Reporting Blue Light","schedule type":"Daily"}' AS parameter  
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 3 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Reporting Summary and Activity' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Reporting Summary and Activity","schedule type":"Daily"}' AS parameter 
# MAGIC   UNION
# MAGIC
# MAGIC
# MAGIC   SELECT 4 AS dependency_id, 
# MAGIC          'Power BI Report' as dependency_type, 
# MAGIC          'ASDA Rewards Reporting Vouchers Report' AS dependency_name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Reporting Vouchers","schedule type":"Daily"}' AS parameter 
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 5 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Reporting Earn on Fuel' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Reporting Earn on Fuel","schedule type":"Daily"}' AS parameter  
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 6 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_summary_activity_notebooks${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_summary_activity_notebooks" ,"schedule type":"Daily"}' AS parameter  
# MAGIC   
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT 7 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Reporting GHS Voucher Shortfall' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"Asda Rewards Reporting GHS Voucher Shortfall","schedule type":"Daily"}' AS parameter
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 8 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_ghs_shortfall_voucher${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_ghs_shortfall_voucher" ,"schedule type":"Daily"}' AS parameter  
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 9 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_earn_on_fuel${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_earn_on_fuel" ,"schedule type":"Daily"}' AS parameter  
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 10 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_crm_send_level_engine${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_crm_send_level_engine" ,"schedule type":"Daily"}' AS parameter 
# MAGIC
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT 11 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'Send Level Report' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"Send Level Report","schedule type":"Daily"}' AS parameter 
# MAGIC
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT 12 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Supplier Billing' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Supplier Billing","schedule type":"Twice Daily"}' AS parameter 
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 13 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_ghs_participation${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_ghs_participation" ,"schedule type":"Daily"}' AS parameter 
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 14 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'GHS Reward Participation' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"GHS Reward Participation","schedule type":"Daily"}' AS parameter  
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 15 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'ASDA Rewards Trading Dashboard' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Trading Dashboard","schedule type":"Daily"}' AS parameter  
# MAGIC
# MAGIC   UNION   
# MAGIC
# MAGIC   SELECT 16 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_crm_send_level_engine_store${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_crm_send_level_engine_store" ,"schedule type":"Daily"}' AS parameter 
# MAGIC   UNION       
# MAGIC
# MAGIC
# MAGIC   SELECT 17 AS dependency_id, 
# MAGIC          'Power BI Report' as type, 
# MAGIC          'Send Level Report Rewards' AS name,
# MAGIC          '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"Send Level Report Rewards","schedule type":"Daily"}' AS parameter       
# MAGIC            
# MAGIC  UNION
# MAGIC  
# MAGIC  SELECT 18 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'ASDA Rewards Business Case Dashboard' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Business Case Dashboard","schedule type":"Daily"}' AS parameter      
# MAGIC           
# MAGIC  UNION
# MAGIC  
# MAGIC  SELECT 19 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'ASDA Rewards Daily Store Dashboard' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Daily Store Dashboard","schedule type":"Daily"}' AS parameter      
# MAGIC           
# MAGIC  UNION
# MAGIC  
# MAGIC  SELECT 20 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'ASDA Rewards GSM Dashboard' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards GSM Dashboard","schedule type":"Daily"}' AS parameter      
# MAGIC           
# MAGIC  UNION
# MAGIC  
# MAGIC  SELECT 21 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'ASDA Rewards Store Tracker' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Store Tracker","schedule type":"Daily"}' AS parameter      
# MAGIC           
# MAGIC  UNION
# MAGIC  
# MAGIC  SELECT 22 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'ASDA Rewards Loss Prevention Summary Dashboard' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Loss Prevention Summary Dashboard","schedule type":"Daily"}' AS parameter      
# MAGIC           
# MAGIC  UNION
# MAGIC  
# MAGIC  SELECT 23 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'ASDA Rewards Loss Prevention Customer Detail Dashboard' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"ASDA Rewards Loss Prevention Customer Detail Dashboard","schedule type":"Daily"}' AS parameter      
# MAGIC           
# MAGIC   UNION   
# MAGIC
# MAGIC   SELECT 24 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_crm_monthly_ghs${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_crm_monthly_ghs" ,"schedule type":"Daily"}' AS parameter 
# MAGIC   UNION   
# MAGIC
# MAGIC   SELECT 25 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_crm_monthly_george${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_crm_monthly_george" ,"schedule type":"Daily"}' AS parameter         
# MAGIC   
# MAGIC   UNION
# MAGIC
# MAGIC   SELECT 26 AS dependency_id,
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'cdna_ar_daily_store${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"cdna_ar_daily_store" ,"schedule type":"Daily"}' AS parameter   
# MAGIC
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT 27 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_key_objectives${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_key_objectives" ,"schedule type":"Daily"}' AS parameter
# MAGIC
# MAGIC   UNION
# MAGIC  
# MAGIC   SELECT 28 AS dependency_id,
# MAGIC           'Power BI Report' as type,
# MAGIC           'LS Eleven Key Objectives' AS name,
# MAGIC           '{"workspace":"LS Eleven - Asda${widget.workspace_suffix}","dataset":"LS Eleven Key Objectives","schedule type":"Daily"}' AS parameter
# MAGIC
# MAGIC   UNION
# MAGIC   
# MAGIC   SELECT 29 AS dependency_id, 
# MAGIC          'Databricks Workflow' as type, 
# MAGIC          'foundations_v1_job_crm_send_level_rewards_george${widget.workflow_suffix}' AS name,
# MAGIC          '{"databricks_workflow_name":"foundations_v1_job_crm_send_level_rewards_george" ,"schedule type":"Daily"}' AS parameter
# MAGIC
# MAGIC    
# MAGIC )
# MAGIC
# MAGIC MERGE INTO ${widget.catalog}.${widget.schema}.data_quality_dependency_definition AS target 
# MAGIC USING check_dependency_definition_cte AS source
# MAGIC ON source.dependency_id = target.dependency_id
# MAGIC AND source.type = target.type
# MAGIC AND source.name = target.name
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE;
# MAGIC     
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.data_quality_dependency_definition LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  WITH check_dependency_cte AS
# MAGIC  (
# MAGIC           --PBI Report - Earn and Burn Report Dependency
# MAGIC           SELECT 1 AS dependency_id, 1 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 2 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 3 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 4 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 5 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 6 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 9 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 10 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 11 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 12 AS check_id,'Low' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 13 AS check_id,'Low' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 15 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 23 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 1 AS dependency_id, 24 AS check_id,'Low' AS severity UNION
# MAGIC
# MAGIC           --PBI Report - ASDA Rewards Reporting Blue Light
# MAGIC           SELECT 2 AS dependency_id, 1 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 2 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 3 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 4 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 10 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 14 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 15 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 2 AS dependency_id, 24 AS check_id,'Low' AS severity UNION
# MAGIC
# MAGIC           --PBI Report - ASDA Rewards Reporting Summary and Activity
# MAGIC           SELECT 3 AS dependency_id, 1 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 2 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 3 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 4 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 5 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 6 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 9 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 10 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 15 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 18 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 20 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 21 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 22 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 24 AS check_id,'Low' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 25 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 3 AS dependency_id, 26 AS check_id,'High' AS severity UNION
# MAGIC
# MAGIC           --PBI Report - ASDA Rewards Reporting Vouchers Report
# MAGIC           SELECT 4 AS dependency_id, 17 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 4 AS dependency_id, 23 AS check_id,'High' AS severity UNION
# MAGIC           
# MAGIC           --PBI Report - ASDA Rewards Reporting Earn on Fuel
# MAGIC           SELECT 5 AS dependency_id, 8 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 5 AS dependency_id, 31 as check_id,'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - summary_activity_notebooks
# MAGIC           SELECT 6 AS dependency_id, 1 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 2 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 3 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 4 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 10 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 12 AS check_id,'Low' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 15 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 6 AS dependency_id, 24 AS check_id,'Low' AS severity UNION
# MAGIC
# MAGIC           --PBI Report - Asda Rewards Reporting GHS Voucher Shortfall
# MAGIC           SELECT 7 AS dependency_id, 27 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - ghs_shortfall_voucher
# MAGIC           SELECT 8 AS dependency_id, 3 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 8 AS dependency_id, 4 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 8 AS dependency_id, 9 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 8 AS dependency_id, 10 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 8 AS dependency_id, 11 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 8 AS dependency_id, 19 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 8 AS dependency_id, 35 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - earn_on_fuel
# MAGIC           SELECT 9 AS dependency_id, 7 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 9 AS dependency_id, 32 AS check_id,'High' AS severity UNION
# MAGIC           SELECT 9 AS dependency_id, 33 AS check_id,'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - crm_send_level_engine
# MAGIC           SELECT 10 AS dependency_id, 23 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 10 AS dependency_id, 28 AS check_id, 'Low' AS severity UNION
# MAGIC           SELECT 10 AS dependency_id, 29 AS check_id, 'Low' AS severity UNION
# MAGIC
# MAGIC            -- PBI Report- crm_send_level_engine
# MAGIC           SELECT 11 AS dependency_id, 30 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           --PBI Report - Supplier Billing
# MAGIC           SELECT 12 AS dependency_id, 34 as check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           --Databricks Workflow - GHS Reward Participation
# MAGIC           SELECT 13 AS dependency_id, 3 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 13 AS dependency_id, 35 as check_id, 'High' AS severity UNION
# MAGIC  
# MAGIC           --PBI Report - GHS Reward Participation
# MAGIC           SELECT 14 AS dependency_id, 36 as check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           --PBI Report - ASDA Rewards Trading Dashboard
# MAGIC           SELECT 15 AS dependency_id, 1 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 2 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 3 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 4 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 37 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 38 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 39 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 40 as check_id, 'High' AS severity UNION
# MAGIC           SELECT 15 AS dependency_id, 41 as check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - crm_send_level_engine_store
# MAGIC           SELECT 16 AS dependency_id, 37 AS check_id, 'High' AS severity UNION
# MAGIC           
# MAGIC           -- PBI Report- crm_send_level_engine_store
# MAGIC           SELECT 17 AS dependency_id, 42 AS check_id, 'High' AS severity UNION
# MAGIC           
# MAGIC           -- PBI Report - ASDA Rewards Business Case Dashboard
# MAGIC           SELECT 18 AS dependency_id, 43 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 44 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 45 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 46 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 47 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 48 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 49 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 50 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 51 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 52 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 53 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 54 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 55 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 56 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 57 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 58 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 59 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 60 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 18 AS dependency_id, 62 AS check_id, 'High' AS severity UNION
# MAGIC           
# MAGIC           -- PBI Report - ASDA Rewards Daily Store Dashboard
# MAGIC           SELECT 19 AS dependency_id, 61 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 19 AS dependency_id, 63 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- PBI Report - ASDA Rewards GSM Dashboard
# MAGIC           SELECT 20 AS dependency_id, 43 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 44 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 45 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 46 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 47 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 48 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 49 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 50 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 51 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 52 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 53 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 54 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 55 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 56 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 57 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 58 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 59 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 60 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 20 AS dependency_id, 62 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- PBI Report - ASDA Rewards Store Tracker
# MAGIC           SELECT 21 AS dependency_id, 43 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 44 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 45 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 46 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 47 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 48 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 49 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 50 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 51 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 52 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 53 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 54 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 55 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 56 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 57 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 58 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 59 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 60 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 21 AS dependency_id, 62 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- PBI Report - ASDA Rewards Loss Prevention Customer Detail Dashboard
# MAGIC           SELECT 22 AS dependency_id, 43 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 44 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 45 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 46 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 47 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 48 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 49 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 50 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 51 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 52 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 53 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 54 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 55 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 56 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 57 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 58 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 59 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 60 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 22 AS dependency_id, 62 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- PBI Report - ASDA Rewards Loss Prevention Summary Dashboard
# MAGIC           SELECT 23 AS dependency_id, 43 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 44 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 45 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 46 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 47 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 48 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 49 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 50 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 51 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 52 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 53 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 54 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 55 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 56 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 57 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 58 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 59 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 60 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 23 AS dependency_id, 62 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - crm_monthly_ghs
# MAGIC           SELECT 24 AS dependency_id, 23 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - crm_monthly_george
# MAGIC           SELECT 25 AS dependency_id, 28 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - cdna_ar_daily_store
# MAGIC           SELECT 26 AS dependency_id, 66 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 26 AS dependency_id, 67 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - key_objectives
# MAGIC           SELECT 27 AS dependency_id, 1 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 27 AS dependency_id, 2 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 27 AS dependency_id, 3 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 27 AS dependency_id, 4 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 27 AS dependency_id, 11 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 27 AS dependency_id, 24 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- PBI Report - LS Eleven Key Objectives
# MAGIC           SELECT 28 AS dependency_id, 68 AS check_id, 'High' AS severity UNION
# MAGIC
# MAGIC           -- Databricks Workflow - crm_send_level_rewards_george
# MAGIC           SELECT 29 AS dependency_id, 28 AS check_id, 'High' AS severity UNION
# MAGIC           SELECT 29 AS dependency_id, 69 AS check_id, 'High' AS severity 
# MAGIC
# MAGIC
# MAGIC  )
# MAGIC
# MAGIC MERGE INTO ${widget.catalog}.${widget.schema}.data_quality_dependency_mapping AS target 
# MAGIC USING check_dependency_cte AS source
# MAGIC ON source.dependency_id = target.dependency_id
# MAGIC AND source.check_id = target.check_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC   DELETE;
# MAGIC   
# MAGIC   SELECT * FROM ${widget.catalog}.${widget.schema}.data_quality_dependency_mapping LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###### Remove any checks from the dependency mapping that we have turned off

# COMMAND ----------

#Now remove any dependency which contains an 'inactive' test, to make sure historic tests dont impact current current checks.

inactive_checks = spark.sql(f"""SELECT check_id as c_id FROM {catalog}.{schema}.data_quality_check_definition
                            WHERE active is false""")

mapping_df = spark.sql(f"""
                  SELECT * FROM {catalog}.{schema}.data_quality_dependency_mapping
                  """)
# Now remove relation to turned off checks:
mapping_df = (mapping_df.join(inactive_checks, mapping_df.check_id == inactive_checks.c_id, 'left_anti')
                        .orderBy(mapping_df.dependency_id, mapping_df.check_id))

mapping_df.createOrReplaceTempView("mapping_df")

display(spark.sql(f"""INSERT OVERWRITE {catalog}.{schema}.data_quality_dependency_mapping TABLE mapping_df;"""))
