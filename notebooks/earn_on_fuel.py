# Databricks notebook source
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
# MAGIC ###Transformations to build Earn On Fuel output tables

# COMMAND ----------

import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ###Temporary view for date range

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW eof_date_range
# MAGIC AS
# MAGIC SELECT 
# MAGIC     DATE_SUB(CURRENT_DATE(),1) AS run_date,
# MAGIC     '2023-12-06' AS start_date,
# MAGIC     DATE_SUB('2023-12-06', 91) AS start_date_minus13wk,
# MAGIC     DATE_SUB(CURRENT_DATE(), 8) AS last_7d,
# MAGIC     DATE_SUB(CURRENT_DATE(), 31) AS last_30d,
# MAGIC     DATE_SUB(CURRENT_DATE(), 92) AS last_91d;
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from eof_date_range

# COMMAND ----------

# MAGIC %md
# MAGIC ##PFS Visit level metrics

# COMMAND ----------

# MAGIC %md
# MAGIC #####Creating dataframes using source tables

# COMMAND ----------

df_date_range = spark.table("eof_date_range")
df_pfs_sales = spark.table("${widget.core_catalog}.gb_customer_data_domain_odl.cdd_odl_pfs_sales")
df_cal = spark.table("${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Limiting data to Stevenage stores and grouping petrol and diesel sales
# MAGIC There is filter applied to visit date to not include data from 11th Jan 2024 to 16th Jan 2024 as the earn on fuel functionality was disabled suring this time

# COMMAND ----------

windowSpec = Window.partitionBy("visit_dt", "trans_nbr", "store_nbr", "reg_nbr").orderBy(F.desc(F.abs(F.col("sales_adj").cast("float"))))

df_pfs_sales = (
    df_pfs_sales.filter(
        (F.col("store_nbr") == 4582) & (F.col("upc_nbr").like("%FUEL%")) 
        )
    .withColumn("vis_rank", F.rank().over(windowSpec))
    .select("visit_dt", "trans_nbr", "store_nbr", "reg_nbr",
			"store_type",
			"wallet_id",
			"upc_nbr",
			"sale_amt_inc_vat",
			"qty",
			F.abs(F.col("sales_adj").cast("float")).alias("sales_adj"),
            "vis_rank")
    .distinct()
    .join(df_date_range, on = [df_pfs_sales.visit_dt.between(df_date_range.start_date_minus13wk, df_date_range.run_date)] , how = "inner")
    .filter((F.col("vis_rank") == 1) & (~F.col("visit_dt").between("2024-01-11","2024-01-16") ))
    .join(df_cal, on = [df_pfs_sales.visit_dt == df_cal.day_date], how = "leftouter")
    .withColumn("prod_type", F.when(F.col("upc_nbr") == "FUEL1", "petrol").when(F.col("upc_nbr") == "FUEL2", "diesel").when(F.col("upc_nbr").isin({"FUEL0","FUEL3"}), "other_fuel").otherwise("non_fuel"))
    .withColumn("ar_visit",F.when(F.col("wallet_id").isNotNull(), 1).otherwise(0))
    .select(F.col("asda_wk_nbr").alias("visit_week"),"visit_dt", "trans_nbr", "store_nbr", "reg_nbr",
			"store_type",
			"wallet_id",
			"upc_nbr",
			"sale_amt_inc_vat",
			"qty",
			"sales_adj",
            "prod_type",
            "ar_visit"
   )
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Aggregation at visit level
# MAGIC
# MAGIC Metrics such as total_sales, total_units, ar_sales, non_ar_sales, non_ar_units, ar_units calculated

# COMMAND ----------

df_pfs_visit = (
    df_pfs_sales.groupBy("visit_week",
	"visit_dt", 
	"trans_nbr", 
	"store_nbr",
	"reg_nbr",
	"store_type",
	"wallet_id",
	"prod_type",
    "ar_visit")
    .agg(F.sum("sale_amt_inc_vat").alias("total_sales"),
          F.sum("qty").alias("total_units"),
          F.sum(F.when(F.col("wallet_id").isNull(), F.col("sale_amt_inc_vat")).otherwise(0)).alias("non_ar_sales"),
          F.sum(F.when(F.col("wallet_id").isNull(), F.col("qty")).otherwise(0)).alias("non_ar_units"),
          F.sum(F.when(F.col("wallet_id").isNotNull(), F.col("sale_amt_inc_vat")).otherwise(0)).alias("ar_sales"),
          F.sum(F.when(F.col("wallet_id").isNotNull(), F.col("qty")).otherwise(0)).alias("ar_units"),
          F.sum("sales_adj").alias("earn")
    )

)

# COMMAND ----------

# MAGIC %md
# MAGIC #####New Signups: Flag wallet_ids that are completely new to the Rewards scheme
# MAGIC first_ar_visit is a flag that represents if its first ever visit under Rewards

# COMMAND ----------

windowSpec1 = Window.partitionBy("wallet_id").orderBy("visit_dt", "trans_nbr", "reg_nbr", F.desc("store_nbr"))
df_wallet_pos_txns = spark.table("${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns")
df_first_visit = (df_wallet_pos_txns.select("wallet_id", "visit_dt", "trans_nbr", "reg_nbr", "store_nbr")
                  .withColumn("first_ar_visit", F.rank().over(windowSpec1))
                  .filter(F.col("first_ar_visit") == 1))

# COMMAND ----------

# MAGIC %md
# MAGIC Calcuating visit date minus 7d, 30d and 13wk for other calculations further down

# COMMAND ----------

va = df_pfs_visit.alias("va")
fv = df_first_visit.alias("fv")
cond = [fv.wallet_id == va.wallet_id, fv.visit_dt == va.visit_dt, fv.trans_nbr == va.trans_nbr, fv.reg_nbr == va.reg_nbr, fv.store_nbr == va.store_nbr]
df_pfs_helper_columns = (va.join(fv, on = cond, how = "leftouter")
      .withColumn("visit_minus_7d",F.date_sub(va.visit_dt, 7))
      .withColumn("visit_minus_30d",F.date_sub(va.visit_dt, 30))
      .withColumn("visit_minus13wk",F.date_sub(va.visit_dt, 91))
      .select("va.*","first_ar_visit", "visit_minus_7d", "visit_minus_30d", "visit_minus13wk")
)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Flag fuel visits in the last day, 7 days, 30 days and 13 weeks

# COMMAND ----------

df = df_pfs_helper_columns.crossJoin(df_date_range)
df_pfs_visit_agg = (
    df.withColumn("active_1d",F.when(F.col("visit_dt") == F.col("run_date"), 1).otherwise(0))
      .withColumn("active_7d",F.when(((F.col("last_7d") <= F.col("visit_dt")) & (F.col("visit_dt") <= F.col("run_date"))), 1).otherwise(0))
      .withColumn("active_30d",F.when(((F.col("last_30d") <= F.col("visit_dt")) & (F.col("visit_dt") <= F.col("run_date"))), 1).otherwise(0))
      .withColumn("active_13wk",F.when(((F.col("last_91d") <= F.col("visit_dt")) & (F.col("visit_dt") <= F.col("run_date"))), 1).otherwise(0)))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer Level Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC #####Aggregation at wallet_id level for Asda Rewards Customer

# COMMAND ----------

df_pfs_ar_cust = (
    df_pfs_visit_agg.filter(F.col("wallet_id").isNotNull())
    .groupBy(F.col("wallet_id"))
    .agg(F.countDistinct(F.col("visit_week")).alias("shopped_weeks"),
    F.countDistinct(F.concat("visit_dt", "trans_nbr", "reg_nbr", "store_nbr")).alias("total_visits"),
    F.sum(F.col("total_sales")).alias("total_sales"),
    F.sum(F.col("total_units")).alias("total_units"),
    F.sum(F.col("ar_sales")).alias("ar_sales"),
    F.sum(F.col("ar_units")).alias("ar_units"),
    F.sum(F.col("ar_visit")).alias("ar_visits"),
    F.sum(F.col("earn")).alias("earn"),
    F.sum(F.col("first_ar_visit")).alias("first_ar_visit"),
    F.max(F.col("active_1d")).alias("active_1d"),
    F.max(F.col("active_7d")).alias("active_7d"),
    F.max(F.col("active_30d")).alias("active_30d"),
    F.max(F.col("active_13wk")).alias("active_13wk")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Get all in-store visits for EOF customers within the relevant date range

# COMMAND ----------

df_store_visit = spark.table("${widget.core_catalog}.gb_mb_store_dl_tables.store_visit ")
df_cal = spark.table("${widget.core_catalog}.gb_customer_data_domain_rpt.cdd_rpt_dim_calendar")
df_wallet_pos_txns = spark.table("${widget.core_catalog}.gb_mb_dl_tables.wallet_pos_txns")
wal = df_wallet_pos_txns.alias("wal")
ar = df_pfs_ar_cust.alias("ar")
sv = df_store_visit.alias("sv")

df_store = (
        wal.filter(
        (F.col("store_nbr") == 4582) & (F.col("chnl_nm") == "store") & (~ F.col("trans_rcpt_nbr").like("%ZD%")) &  (~ F.col("trans_rcpt_nbr").like("%WMT%")))
        .join(df_date_range, [wal.visit_dt.between(df_date_range.start_date_minus13wk, df_date_range.run_date)],"inner")
        .select("wallet_id", "visit_dt", "trans_nbr", "store_nbr", "reg_nbr")
)

df_ar_store= (
    df_store.join(ar, on  = [df_store.wallet_id == ar.wallet_id], how = "inner")
    .select(df_store.wallet_id, "visit_dt", "trans_nbr", "store_nbr", "reg_nbr")
)

rs = df_ar_store.alias("rs")

cond = [rs.store_nbr == sv.store_nbr , rs.reg_nbr == sv.reg_nbr, rs.trans_nbr == sv.trans_nbr, rs.visit_dt == sv.visit_dt]

df_cust_store_visits = (
    rs.join(sv, on = cond, how = "inner")
    .select(rs.wallet_id,
            rs.visit_dt.alias("sv_visit_dt"),
            rs.trans_nbr.alias("sv_trans_nbr"),
            rs.store_nbr.alias("sv_store_nbr"),
            rs.reg_nbr.alias("sv_reg_nbr"),
            sv.visit_nbr.alias("sv_visit_nbr"), 
            sv.tot_visit_amt.alias("sv_ar_sales"), 
            sv.tot_scan_cnt.alias("sv_ar_units"))
)

csv = df_cust_store_visits.alias("csv")
df_cust_store_visits = (
    csv.join(df_cal, on = [csv.sv_visit_dt == df_cal.day_date], how = "inner")
    .select("csv.*", df_cal.asda_wk_nbr.alias("sv_visit_week"))
)



# COMMAND ----------

# MAGIC %md
# MAGIC #####Append store visits to fuel visits at wallet_id level

# COMMAND ----------

fv = df_pfs_visit_agg.alias("fv")
sv = df_cust_store_visits.alias("sv")

df_cust_all_visits = (
    fv.join(sv, on = [fv.wallet_id == sv.wallet_id], how = "leftouter")
    .drop(sv.wallet_id)
    .where(sv.sv_visit_dt <= fv.visit_dt)
    .withColumn("cross_shop_1d", F.when(fv.visit_dt == sv.sv_visit_dt, 1).otherwise(0))
    .withColumn("cross_shop_7d", F.when((fv.visit_minus_7d <=  sv.sv_visit_dt) & (sv.sv_visit_dt < fv.visit_dt), 1).otherwise(0))
    .withColumn("cross_shop_30d", F.when((fv.visit_minus_30d <=  sv.sv_visit_dt) & (sv.sv_visit_dt < fv.visit_minus_7d), 1).otherwise(0))
    .withColumn("cross_shop_13wk", F.when((fv.visit_minus13wk <=  sv.sv_visit_dt) & (sv.sv_visit_dt < fv.visit_minus_30d), 1).otherwise(0))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Cross shop metrics at fuel visit level

# COMMAND ----------

visits = (
    df_cust_all_visits.groupBy("visit_dt",
		"trans_nbr",
		"reg_nbr",
		"store_nbr",
		"wallet_id",
		"ar_sales",
		"ar_units",
		"earn",
		"first_ar_visit")
        .agg(
        F.sum(F.col("cross_shop_1d")).alias("sv_1d"),
		F.sum(F.col("cross_shop_7d")).alias("sv_7d"),
		F.sum(F.col("cross_shop_30d")).alias("sv_30d"),
		F.sum(F.col("cross_shop_13wk")).alias("sv_13wk"),
		F.sum(F.when(F.col("cross_shop_1d") == 1, F.col("sv_ar_sales")).otherwise(0)).alias("sv_ar_sales_1d"),
		F.sum(F.when(F.col("cross_shop_1d") == 1, F.col("sv_ar_units")).otherwise(0)).alias("sv_ar_units_1d"),
		F.sum(F.when(F.col("cross_shop_7d") == 1, F.col("sv_ar_sales")).otherwise(0)).alias("sv_ar_sales_7d"),
		F.sum(F.when(F.col("cross_shop_7d") == 1, F.col("sv_ar_units")).otherwise(0)).alias("sv_ar_units_7d"),
		F.sum(F.when(F.col("cross_shop_30d") == 1, F.col("sv_ar_sales")).otherwise(0)).alias("sv_ar_sales_30d"),
		F.sum(F.when(F.col("cross_shop_30d") == 1, F.col("sv_ar_units")).otherwise(0)).alias("sv_ar_units_30d"),
		F.sum(F.when(F.col("cross_shop_13wk") == 1, F.col("sv_ar_sales")).otherwise(0)).alias("sv_ar_sales_13wk"),
		F.sum(F.when(F.col("cross_shop_13wk") == 1, F.col("sv_ar_units")).otherwise(0)).alias("sv_ar_units_13wk")
        )
)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Flagging if there are store visits in last 1day, 7day, 30d and 13wk. 
# MAGIC Based on it, sales and units are calculated for corresponding days.

# COMMAND ----------

df_visits_cross_shop_agg = (
    visits.withColumn("sv_1d_flag", F.when(F.col("sv_1d") > 0 ,1).otherwise(0))
    .withColumn("sv_7d_flag", F.when((F.col("sv_1d") == 0) & (F.col("sv_7d") > 0),1).otherwise(0))
    .withColumn("sv_30d_flag", F.when((F.col("sv_1d") == 0) & (F.col("sv_7d") == 0) &(F.col("sv_30d") > 0),1).otherwise(0))
    .withColumn("sv_13wk_flag", F.when((F.col("sv_1d") == 0) & (F.col("sv_7d") == 0) & (F.col("sv_30d") == 0) &(F.col("sv_13wk") > 0),1).otherwise(0))
    .withColumn("cs_spv_1d", F.col("sv_ar_sales_1d")/F.col("sv_1d"))
    .withColumn("cs_upv_1d", F.col("sv_ar_units_1d")/F.col("sv_1d"))
    .withColumn("cs_spv_7d", F.col("sv_ar_sales_7d")/F.col("sv_7d"))
    .withColumn("cs_upv_7d", F.col("sv_ar_units_7d")/F.col("sv_7d"))
    .withColumn("cs_spv_30d", F.col("sv_ar_sales_30d")/F.col("sv_30d"))
    .withColumn("cs_upv_30d", F.col("sv_ar_units_30d")/F.col("sv_30d"))
    .withColumn("cs_spv_13wk", F.col("sv_ar_sales_13wk")/F.col("sv_13wk"))
    .withColumn("cs_upv_13wk", F.col("sv_ar_units_13wk")/F.col("sv_13wk"))
    .drop("sv_1d", "sv_7d", "sv_30d", "sv_13wk", "sv_ar_sales_1d", "sv_ar_units_1d", "sv_ar_sales_7d", "sv_ar_units_7d", "sv_ar_sales_30d", "sv_ar_units_30d", "sv_ar_sales_13wk", "sv_ar_units_13wk")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Appending cross shop metrics to visits summary data

# COMMAND ----------

cs = df_visits_cross_shop_agg.alias("cs")
pfs = df_pfs_helper_columns.alias("pfs")
windowSpec = Window.partitionBy("wallet_id").orderBy("visit_dt", "trans_nbr", "reg_nbr", F.desc("store_nbr"))
eof_visits = (
    pfs.filter(F.col("wallet_id").isNotNull())
    .withColumn("eof_visit_ranking", F.rank().over(windowSpec))
    .select("wallet_id", "visit_dt", "trans_nbr", "reg_nbr", "store_nbr", "eof_visit_ranking")
)
cond = [pfs.visit_dt == cs.visit_dt, pfs.trans_nbr == cs.trans_nbr, pfs.reg_nbr == cs.reg_nbr, pfs.store_nbr == cs.store_nbr]
visits_summary = (
    pfs.join(cs, cond , how = "leftouter")
    .select("pfs.*", cs.sv_1d_flag, cs.sv_7d_flag, cs.sv_30d_flag, cs.sv_13wk_flag, cs.cs_spv_1d, cs.cs_spv_7d, cs.cs_spv_30d, cs.cs_spv_13wk)
)
visits_summary.drop(F.col("visit_minus_7d"), F.col("visit_minus_30d"), F.col("visit_minus13wk"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Calculating store visit ranking per wallet_id

# COMMAND ----------

vs = visits_summary.alias("vs")
eof = eof_visits.alias("eof")
cond = [vs.visit_dt == eof.visit_dt, vs.trans_nbr == eof.trans_nbr, vs.reg_nbr == eof.reg_nbr, vs.store_nbr == eof.store_nbr]
df_pfs_visits_summary = (
    vs.join(eof, cond, how = "leftouter").select("vs.*", eof.eof_visit_ranking))


# COMMAND ----------

# MAGIC %md
# MAGIC ##Cross shop metrics at customer level

# COMMAND ----------

# MAGIC %md
# MAGIC #####Number of in-store cross shop visits per wallet_id

# COMMAND ----------

df_custs_cross_shop_agg = (
    df_visits_cross_shop_agg.groupBy(F.col("wallet_id"))
    .agg(
        F.countDistinct(F.when(F.col("sv_1d_flag") == 1, F.concat(F.col("visit_dt"), F.col("trans_nbr"), F.col("reg_nbr"), F.col("store_nbr"))).otherwise(None)).alias("fv_cross_shop_1d"),
        F.countDistinct(F.when(F.col("sv_7d_flag") == 1, F.concat(F.col("visit_dt"), F.col("trans_nbr"), F.col("reg_nbr"), F.col("store_nbr"))).otherwise(None)).alias("fv_cross_shop_7d"),
        F.countDistinct(F.when(F.col("sv_30d_flag") == 1, F.concat(F.col("visit_dt"), F.col("trans_nbr"), F.col("reg_nbr"), F.col("store_nbr"))).otherwise(None)).alias("fv_cross_shop_30d"),
        F.countDistinct(F.when(F.col("sv_13wk_flag") == 1, F.concat(F.col("visit_dt"), F.col("trans_nbr"), F.col("reg_nbr"), F.col("store_nbr"))).otherwise(None)).alias("fv_cross_shop_13wk")
     
    )
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Earn On Fuel Output Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #####Truncating tables for fresh load

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE ${widget.catalog}.${widget.schema}.eof_customer_level;
# MAGIC TRUNCATE TABLE ${widget.catalog}.${widget.schema}.eof_visit_level;
# MAGIC TRUNCATE TABLE ${widget.catalog}.${widget.schema}.eof_prod_level;

# COMMAND ----------

df_by_prod = (df_pfs_visits_summary.groupBy("prod_type")
              .agg(F.countDistinct(vs.wallet_id).alias("eof_custs"))
)
df_by_prod.write.insertInto("${widget.catalog}.${widget.schema}.eof_prod_level")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Customer Level Output

# COMMAND ----------

ar = df_pfs_ar_cust.alias("ar")
cus = df_custs_cross_shop_agg.alias("cus")
df_pfs_customer_summary = (
    ar.join(cus, on = [ar.wallet_id == cus.wallet_id], how = "leftouter")
    .withColumn("cust_cross_shop_1d",F.when((F.col("fv_cross_shop_1d") > 0), 1).otherwise(0))
    .withColumn("cust_cross_shop_7d",F.when((F.col("fv_cross_shop_1d") == 0) & (F.col("fv_cross_shop_7d") > 0), 1).otherwise(0))
    .withColumn("cust_cross_shop_30d",F.when((F.col("fv_cross_shop_1d") == 0) & (F.col("fv_cross_shop_7d") == 0) & (F.col("fv_cross_shop_30d") > 0) , 1).otherwise(0))
    .withColumn("cust_cross_shop_13wk",F.when((F.col("fv_cross_shop_1d") == 0) & (F.col("fv_cross_shop_7d") == 0) & (F.col("fv_cross_shop_30d") == 0) & (F.col("fv_cross_shop_13wk") > 0), 1).otherwise(0))
    .select(ar.wallet_id, ar.shopped_weeks, ar.total_visits, ar.total_sales, ar.total_units,ar.ar_sales, ar.ar_units, ar.ar_visits, ar.earn, ar.first_ar_visit, ar.active_1d, ar.active_7d, ar.active_30d, ar.active_13wk,  cus.fv_cross_shop_1d, cus.fv_cross_shop_7d, cus.fv_cross_shop_30d, cus.fv_cross_shop_13wk, "cust_cross_shop_1d", "cust_cross_shop_7d", "cust_cross_shop_30d", "cust_cross_shop_13wk" ) 
)
df_pfs_customer_summary.write.insertInto("${widget.catalog}.${widget.schema}.eof_customer_level")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Visit Level Output

# COMMAND ----------

df_daily_by_product = (
    df_pfs_visits_summary.filter(vs.visit_dt >= "2023-12-06")
    .groupBy(vs.store_nbr,vs.visit_dt, F.col("prod_type"))
    .agg(
        F.countDistinct(vs.wallet_id).alias("eof_custs"),
        F.countDistinct(F.when(F.col("eof_visit_ranking") == 1, vs.wallet_id).otherwise(None)).alias("dist_eof_custs"),
	    F.countDistinct(F.concat(vs.visit_dt, vs.trans_nbr, vs.reg_nbr, vs.store_nbr)).alias("visits"),
	    F.sum(F.col("total_sales")).alias("total_sales"),
	    F.sum(F.col("total_units")).alias("total_units"),
	    F.countDistinct(F.when(F.col("ar_visit") == 1, F.concat(vs.visit_dt, vs.trans_nbr, vs.reg_nbr, vs.store_nbr)).otherwise(None)).alias("ar_visits"),
	    F.sum(F.col("ar_sales")).alias("ar_sales"),
	    F.sum(F.col("ar_units")).alias("ar_units"),
	    F.countDistinct(F.when(F.col("ar_visit") == 0, F.concat(vs.visit_dt, vs.trans_nbr, vs.reg_nbr, vs.store_nbr)).otherwise(None)).alias("non_ar_visits"),
	    F.sum(F.col("non_ar_sales")).alias("non_ar_sales"),
	    F.sum(F.col("non_ar_units")).alias("non_ar_units"),
	    F.sum(F.col("earn")).alias("earn"),
	    F.sum(F.col("first_ar_visit")).alias("new_signup"),
	    F.sum(F.col("sv_1d_flag")).alias("cross_shop_1d"),
	    F.sum(F.col("sv_7d_flag")).alias("cross_shop_7d"),
	    F.sum(F.col("sv_30d_flag")).alias("cross_shop_30d"),
	    F.sum(F.col("sv_13wk_flag")).alias("cross_shop_13wk"),
	    F.sum(F.col("cs_spv_1d")).alias("cs_spv_1d"),
	    F.sum(F.col("cs_spv_7d")).alias("cs_spv_7d"),
	    F.sum(F.col("cs_spv_30d")).alias("cs_spv_30d"),
	    F.sum(F.col("cs_spv_13wk")).alias("cs_spv_13wk")
    )    
)
df_daily_by_product.write.insertInto("${widget.catalog}.${widget.schema}.eof_visit_level")

