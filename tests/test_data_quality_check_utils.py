import pytest

from pyspark.testing import assertDataFrameEqual
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, BooleanType, FloatType, StringType, LongType, DoubleType, TimestampType
import datetime

import sys
sys.path.append('../')
from data_quality_notebooks.data_quality_check_utils import *


def test_PercentageOfMedian_with_correct_data(spark, sample_daily_row_count_full_data):
    metric = PercentageOfMedian()
    tolerance = 0.25
    metric.set_params(sample_daily_row_count_full_data, tolerance)
    output = metric.calc_cutoff()

    expected_schema = StructType([StructField('_1', StringType(), True), 
                                StructField('_2', LongType(), True),
                                StructField('low_count_cutoff', DoubleType(), False),
                                StructField('pass_check', BooleanType(), False)])

    expected_result = spark.createDataFrame([['2024-02-05', 17, 286.0, False],
                                            ['2024-02-04', 2301, 286.0, True],
                                            ['2024-02-03', 1370, 286.0, True],
                                            ['2024-02-02', 1266, 286.0, True],
                                            ['2024-02-01', 1125, 286.0, True],
                                            ['2024-01-31', 1604, 286.0, True],
                                            ['2024-01-30', 1179, 286.0, True],
                                            ['2024-01-29', 1163, 286.0, True],
                                            ['2024-01-28', 1311, 286.0, True],
                                            ['2024-01-27', 1508, 286.0, True],
                                            ['2024-01-26', 1455, 286.0, True],
                                            ['2024-01-25', 1554, 286.0, True],
                                            ['2024-01-24', 878, 286.0, True],
                                            ['2024-01-23', 745, 286.0, True],
                                            ['2024-01-22', 577, 286.0, True],
                                            ['2024-01-21', 716, 286.0, True],
                                            ['2024-01-20', 792, 286.0, True],
                                            ['2024-01-19', 787, 286.0, True],
                                            ['2024-01-18', 826, 286.0, True],
                                            ['2024-01-17', 826, 286.0, True]], expected_schema)
    
    assertDataFrameEqual(output, expected_result)

def test_PercentageOfMedian_with_incorrect_data(spark, sample_daily_row_count_missing_data):
    metric = PercentageOfMedian()
    tolerance = 0.25
    metric.set_params(sample_daily_row_count_missing_data, tolerance)
    output = metric.calc_cutoff()

    expected_schema = StructType([StructField('_1', StringType(), True), 
                                StructField('_2', LongType(), True),
                                StructField('low_count_cutoff', DoubleType(), False),
                                StructField('pass_check', BooleanType(), False)])

    expected_result = spark.createDataFrame([['2024-02-05', 17, 250.375, False],
                                            ['2024-02-04', 10, 250.375, False],
                                            ['2024-02-03', 1370, 250.375, True],
                                            ['2024-02-02', 1266, 250.375, True],
                                            ['2024-02-01', 1125, 250.375, True],
                                            ['2024-01-31', 1604, 250.375, True],
                                            ['2024-01-30', 1179, 250.375, True],
                                            ['2024-01-29', 1163, 250.375, True],
                                            ['2024-01-28', 1311, 250.375, True],
                                            ['2024-01-27', 1508, 250.375, True],
                                            ['2024-01-26', 1455, 250.375, True],
                                            ['2024-01-25', 1554, 250.375, True],
                                            ['2024-01-24', 878, 250.375, True],
                                            ['2024-01-23', 745, 250.375, True],
                                            ['2024-01-22', 577, 250.375, True],
                                            ['2024-01-21', 716, 250.375, True],
                                            ['2024-01-20', 792, 250.375, True],
                                            ['2024-01-19', 787, 250.375, True],
                                            ['2024-01-18', 826, 250.375, True],
                                            ['2024-01-17', 826, 250.375, True]], expected_schema)
    
    assertDataFrameEqual(output, expected_result)

def test_PercentageOfMedian_with_no_data(spark):
    with pytest.raises(Exception):
        metric = PercentageOfMedian()
        tolerance = 0.25
        
        schema = StructType([StructField('_1', StringType(), True), 
                                StructField('_2', LongType(), True),
                                StructField('low_count_cutoff', DoubleType(), False),
                                StructField('pass_check', BooleanType(), False)])

        no_data_df = spark.createDataFrame([],schema=schema)

        mertic.set_params(no_data_df, tolerance)

def test_load_daily_counts_with_correct_data(spark, sample_wallet_pos):

    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
    query = """SELECT cast(event_ts as date), COUNT(*) as row_count FROM sample_wallet_pos
                GROUP BY cast(event_ts as date)
                ORDER BY cast(event_ts as date) desc"""

    result = load_daily_counts(query, spark)
    result_df = result[0].collect()
    error_msg = result[1]

    expectation = [Row(event_ts=datetime.date(2024, 2, 6), row_count=1),
                    Row(event_ts=datetime.date(2024, 2, 5), row_count=3),
                    Row(event_ts=datetime.date(2024, 2, 4), row_count=1),
                    Row(event_ts=datetime.date(2024, 2, 3), row_count=2),
                    Row(event_ts=datetime.date(2024, 2, 2), row_count=1)]

    assert (expectation == result_df and error_msg is None)
    

def test_load_daily_counts_with_no_data(spark):
    query = """SELECT cast(event_ts as date), COUNT(*) as row_count FROM no_data
                GROUP BY cast(event_ts as date)
                ORDER BY cast(event_ts as date) desc"""

    result = load_daily_counts(query, spark)
    result_df = result[0]
    error_msg = result[1]

    assert (error_msg is not None and result_df is None)


def test_compare_daily_counts_with_correct_data(sample_daily_row_count_full_data, date_for_check):

    result = compare_daily_counts(sample_daily_row_count_full_data, PercentageOfMedian(), 0.25, date_for_check)
    assert result == True

def test_compare_daily_counts_with_incorrect_data(sample_daily_row_count_missing_data, date_for_check):

    result = compare_daily_counts(sample_daily_row_count_missing_data, PercentageOfMedian(), 0.25, date_for_check)
    assert result == False

def test_compare_daily_counts_with_no_entry_for_day(sample_daily_row_count_full_data):
    """No entry for desired date, expect test to fail"""
    result = compare_daily_counts(sample_daily_row_count_full_data, PercentageOfMedian(), 0.25, '2024-02-06')
    assert result == False


def test_run_duplicate_data_test_with_correct_data(spark, sample_wallet_pos):
    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
   
    duplication_tolerance = 0.001
    duplication_scope = 365
    
    query = f"""
                SELECT CAST(event_ts AS date) AS dt, COUNT(*) AS row_count,
                count(distinct(trans_rcpt_nbr)) as trans_rcpt_nbrs
                    
                FROM sample_wallet_pos
                WHERE CAST(event_ts AS date) > DATE_ADD(CURRENT_DATE(), -{duplication_scope})
                GROUP BY CAST(event_ts AS date)
                ORDER BY CAST(event_ts AS date) DESC"""
    
    
    duplicate_data_check_result, error_message = run_duplicate_data_test(query = query,
                                                                         tolerance = duplication_tolerance,
                                                                         spark = spark)
    
    assert (duplicate_data_check_result == True and error_message is None)

def test_run_duplicate_data_test_with_duplicate_data(spark, sample_wallet_pos_with_dupes):
    sample_wallet_pos_with_dupes.createOrReplaceTempView("sample_wallet_pos_with_dupes")
   
    duplication_tolerance = 0.001
    duplication_scope = 365
    
    query = f"""
                SELECT CAST(event_ts AS date) AS dt, COUNT(*) AS row_count,
                count(distinct(trans_rcpt_nbr)) as trans_rcpt_nbrs
                    
                FROM sample_wallet_pos_with_dupes
                WHERE CAST(event_ts AS date) > DATE_ADD(CURRENT_DATE(), -{duplication_scope})
                GROUP BY CAST(event_ts AS date)
                ORDER BY CAST(event_ts AS date) DESC"""
    
    
    duplicate_data_check_result, error_message = run_duplicate_data_test(query = query,
                                                                         tolerance = duplication_tolerance,
                                                                         spark = spark)
    
    assert (duplicate_data_check_result == False and error_message is None)

def test_run_duplicate_data_test_with_no_data(spark):
   
    duplication_tolerance = 0.001
    duplication_scope = 365
    
    query = f"""
                select * from Not a real query"""
    
    
    duplicate_data_check_result, error_message = run_duplicate_data_test(query = query,
                                                                         tolerance = duplication_tolerance,
                                                                         spark = spark)
    
    assert (duplicate_data_check_result == False and error_message is not None)

def test_get_historic_snapshot_no_data(spark):
    with pytest.raises(Exception):
        snapshot = get_historic_snapshot(check_id= -99, catalog = 'some_catalog', schema='some_schema', spark = spark)


def test_parse_historic_snapshot(sample_historical_snapshot):
    result_dict, date = parse_historic_snapshot(sample_historical_snapshot)

    expected_dict = {'min_daily_row_count': 1.0,
                    'mean_daily_row_count': 358389.61041902605,
                    'stddev_daily_row_count': 301038.5958465282,
                    'max_daily_row_count': 1243939.0,
                    'total_row_count': 316458026.0}
    
    expected_date = '2024-02-06'

    assert (result_dict == expected_dict and date == expected_date)

def test_parse_historic_snapshot_with_no_data():
    with pytest.raises(Exception):
        result_dict, date = parse_historic_snapshot(None)

def test_is_value_within_tolerance_expected_behaviour():
    result_1 = is_value_within_tolerance(9,10,0.2)
    result_2 = is_value_within_tolerance(9.5,10.0,0.05)
    assert (result_1 == True and result_2 == False)

def test_compare_historic_counts_matching_data(spark):
    
    tolerance = 0.001

    test_dict = {'min_daily_row_count': 1.0,
                    'mean_daily_row_count': 358388.62,
                    'stddev_daily_row_count': 301032.0,
                    'max_daily_row_count': 1243935.0,
                    'total_row_count': 316458027.0}
    
    target_dict = {'min_daily_row_count': 1.0,
                    'mean_daily_row_count': 358389.61041902605,
                    'stddev_daily_row_count': 301038.5958465282,
                    'max_daily_row_count': 1243939.0,
                    'total_row_count': 316458026.0}

    result = compare_historic_counts(test_dict, target_dict, tolerance, spark)
    assert result == True

def test_compare_historic_counts_different_data(spark):
    
    tolerance = 0.001

    test_dict = {'min_daily_row_count': 1.0,
                    'mean_daily_row_count': 398388.62,
                    'stddev_daily_row_count': 251032.0,
                    'max_daily_row_count': 1843935.0,
                    'total_row_count': 316358027.0}
    
    target_dict = {'min_daily_row_count': 1.0,
                    'mean_daily_row_count': 358389.61041902605,
                    'stddev_daily_row_count': 301038.5958465282,
                    'max_daily_row_count': 1243939.0,
                    'total_row_count': 316458026.0}

    result = compare_historic_counts(test_dict, target_dict, tolerance, spark)
    assert result == False

def test_compare_historic_counts_different_keys(spark):
    with pytest.raises(Exception):
        tolerance = 0.001

        test_dict = {'min_daily_row_counts': 1.0,
                        'mean_daily_row_count': 398388.62,
                        'stddev_daily_row_count': 251032.0,
                        'max_daily_row_count': 1843935.0,
                        'total_row_count': 316358027.0}
        
        target_dict = {'min_daily_row_count': 1.0,
                        'mean_daily_row_count': 358389.61041902605,
                        'stddev_daily_row_count': 301038.5958465282,
                        'max_daily_row_count': 1243939.0,
                        'total_row_count': 316458026.0}

        result = compare_historic_counts(test_dict, target_dict, tolerance, spark)

def test_get_current_aggregations(spark, sample_wallet_pos, date_for_check):
    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
    date = date_for_check

    query = """
            SELECT CAST(event_ts AS date), {0}
                    COUNT(*) AS row_count FROM sample_wallet_pos
                            
            WHERE CAST(event_ts AS date) <= '{{date}}'
            GROUP BY CAST(event_ts AS date)""".format('')

    results = get_current_aggregations(query, date, spark)

    expected_dict = {'mean_daily_row_count': 1.3333333333333333,
                    'max_daily_row_count': 2.0,
                    'min_daily_row_count': 1.0,
                    'stddev_daily_row_count': 0.5773502691896257,
                    'total_row_count': 4.0}

    assert results == expected_dict


def test_date_column_check_todays_data(spark, sample_wallet_pos):
    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
    query = """
        SELECT CAST(event_ts AS date) AS dt FROM sample_wallet_pos
                    GROUP BY CAST(event_ts AS date)
                    ORDER BY CAST(event_ts AS date) DESC
                    limit 20
    """
    date_for_check = "2024-02-06"
    result = date_column_check(query, date_for_check, spark)
    assert result == True

def test_date_column_check_most_recent_data(spark, sample_wallet_pos):
    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
    query = """
        SELECT CAST(event_ts AS date) AS dt FROM sample_wallet_pos
                    GROUP BY CAST(event_ts AS date)
                    ORDER BY CAST(event_ts AS date) DESC
                    limit 20
    """
    date_for_check = "2024-01-25"
    result = date_column_check(query, date_for_check, spark)
    assert result == False

def test_date_column_check_old_data(spark, sample_wallet_pos):
    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
    query = """
        SELECT CAST(event_ts AS date) AS dt FROM sample_wallet_pos
                    GROUP BY CAST(event_ts AS date)
                    ORDER BY CAST(event_ts AS date) DESC
                    limit 20
    """
    date_for_check = "2024-04-04"
    result = date_column_check(query, date_for_check, spark)
    assert result == False

def test_date_column_check_day_old_data(spark, sample_wallet_pos):
    sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
    query = """
        SELECT CAST(event_ts AS date) AS dt FROM sample_wallet_pos
                    GROUP BY CAST(event_ts AS date)
                    ORDER BY CAST(event_ts AS date) DESC
                    limit 20
    """
    date_for_check = "2024-02-07"
    result = date_column_check(query, date_for_check, spark)
    assert result == False

def test_date_column_check_no_dt_col(spark, sample_wallet_pos):
    with pytest.raises(Exception):
        sample_wallet_pos.createOrReplaceTempView("sample_wallet_pos")
        query = """
            SELECT CAST(event_ts AS date) FROM sample_wallet_pos
                        GROUP BY CAST(event_ts AS date)
                        ORDER BY CAST(event_ts AS date) DESC
                        limit 20
        """
        date_for_check = "2024-02-02"
        result = date_column_check(query, date_for_check, spark)







