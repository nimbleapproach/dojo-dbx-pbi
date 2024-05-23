# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to create views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT core_catalog DEFAULT "coreprod";
# MAGIC CREATE WIDGET TEXT catalog DEFAULT "";
# MAGIC CREATE WIDGET TEXT schema DEFAULT "";
# MAGIC CREATE WIDGET TEXT view_name DEFAULT "vw_expiring_cashbanks";

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
# MAGIC SELECT *
# MAGIC , case -- adding index column for sorting visual in powerbi
# MAGIC when (cshbnk_category='£0.00') then 'a'
# MAGIC when (cshbnk_category='£0.01-£0.99') then 'b'
# MAGIC when (cshbnk_category='£1-£1.99') then 'c'
# MAGIC when (cshbnk_category='£2-£2.99') then 'd'
# MAGIC when (cshbnk_category='£3-£3.99') then 'e'
# MAGIC when (cshbnk_category='£4-£4.99') then 'f'
# MAGIC when (cshbnk_category='£5-£5.99') then 'g'
# MAGIC when (cshbnk_category='£6-£6.99') then 'h'
# MAGIC when (cshbnk_category='£7-£7.99') then 'i'
# MAGIC when (cshbnk_category='£8-£8.99') then 'j'
# MAGIC when (cshbnk_category='£9-£9.99') then 'k'
# MAGIC when (cshbnk_category='£10-£10.99') then 'l'
# MAGIC when (cshbnk_category='£11-£11.99') then 'm'
# MAGIC when (cshbnk_category='£12-£12.99') then 'n'
# MAGIC when (cshbnk_category='£13-£13.99') then 'o'
# MAGIC when (cshbnk_category='£14-£14.99') then 'p'
# MAGIC when (cshbnk_category='£15-£15.99') then 'q'
# MAGIC when (cshbnk_category='£16-£16.99') then 'r'
# MAGIC when (cshbnk_category='£17-£17.99') then 's'
# MAGIC when (cshbnk_category='£18-£18.99') then 't'
# MAGIC end as cshbnk_category_order
# MAGIC  FROM (
# MAGIC
# MAGIC     select count(distinct singl_profl_id) as num_people
# MAGIC     , cshbnk_category 
# MAGIC     , x as expiry_category 
# MAGIC     , avg(cshbnk) as average
# MAGIC     from(
# MAGIC         --categorise cashbanks values 
# MAGIC         select *
# MAGIC         , case 
# MAGIC         when (cshbnk=0) then '£0.00'
# MAGIC         when (cshbnk>0 and cshbnk<1) then '£0.01-£0.99'
# MAGIC         when (cshbnk>=1 and cshbnk<2) then '£1-£1.99'
# MAGIC         when (cshbnk>=2 and cshbnk<3) then '£2-£2.99'
# MAGIC         when (cshbnk>=3 and cshbnk<4) then '£3-£3.99'
# MAGIC         when (cshbnk>=4 and cshbnk<5) then '£4-£4.99'
# MAGIC         when (cshbnk>=5 and cshbnk<6) then '£5-£5.99'
# MAGIC         when (cshbnk>=6 and cshbnk<7) then '£6-£6.99'
# MAGIC         when (cshbnk>=7 and cshbnk<8) then '£7-£7.99'
# MAGIC         when (cshbnk>=8 and cshbnk<9) then '£8-£8.99'
# MAGIC         when (cshbnk>=9 and cshbnk<10) then '£9-£9.99'
# MAGIC         when (cshbnk>=10 and cshbnk<11) then '£10-£10.99'
# MAGIC         when (cshbnk>=11 and cshbnk<12) then '£11-£11.99'
# MAGIC         when (cshbnk>=12 and cshbnk<13) then '£12-£12.99'
# MAGIC         when (cshbnk>=13 and cshbnk<14) then '£13-£13.99'
# MAGIC         when (cshbnk>=14 and cshbnk<15) then '£14-£14.99'
# MAGIC         when (cshbnk>=15 and cshbnk<16) then '£15-£15.99'
# MAGIC         when (cshbnk>=16 and cshbnk<17) then '£16-£16.99'
# MAGIC         when (cshbnk>=17 and cshbnk<18) then '£17-£17.99'
# MAGIC         when (cshbnk>=18 and cshbnk<19) then '£18-£18.99'
# MAGIC         end as cshbnk_category
# MAGIC         from(select *
# MAGIC             from(
# MAGIC             --find temporary accounts expiring in 7 days and extract cashbank balance
# MAGIC                 SELECT singl_profl_id
# MAGIC                     , regtn_ts
# MAGIC                     , curr_cash_bnk_pnts_qty*0.01 as cshbnk
# MAGIC                     , TIMESTAMPADD(HOUR, (37*24),regtn_ts ) exp_ts
# MAGIC                     , case when TIMESTAMPADD(HOUR, (37*24), regtn_ts) 
# MAGIC                             BETWEEN CURRENT_TIMESTAMP AND TIMESTAMPADD(HOUR, (24*7),CURRENT_TIMESTAMP) THEN '7' ELSE null
# MAGIC                             END as x
# MAGIC                     , CURRENT_TIMESTAMP as y
# MAGIC                 FROM ${widget.catalog}.${widget.schema}.vw_loyalty_acct
# MAGIC                 where singl_profl_id like 'no%'
# MAGIC
# MAGIC                 UNION ALL
# MAGIC                 --find temporary accounts expiring in 14 days and extract cashbank balance
# MAGIC                 SELECT singl_profl_id
# MAGIC                     , regtn_ts
# MAGIC                     , curr_cash_bnk_pnts_qty*0.01 as cshbnk
# MAGIC                     , TIMESTAMPADD(HOUR, (37*24), regtn_ts) exp_ts
# MAGIC                     , case when TIMESTAMPADD(HOUR, (37*24), regtn_ts) 
# MAGIC                             BETWEEN CURRENT_TIMESTAMP AND TIMESTAMPADD(HOUR, (24*14),CURRENT_TIMESTAMP) THEN '14' ELSE null
# MAGIC                             END as x
# MAGIC                     , CURRENT_TIMESTAMP as y
# MAGIC                 FROM ${widget.catalog}.${widget.schema}.vw_loyalty_acct
# MAGIC                 where singl_profl_id like 'no%'
# MAGIC             )
# MAGIC             where x is not null)
# MAGIC
# MAGIC     )
# MAGIC     group by cshbnk_category, expiry_category
# MAGIC     order by expiry_category, cshbnk_category
# MAGIC );
# MAGIC
# MAGIC
# MAGIC   --SELECT * FROM ${complete_view_name} LIMIT 10;
# MAGIC
# MAGIC
