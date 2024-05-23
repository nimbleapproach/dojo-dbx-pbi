"""
This module contains pytest fixtures which provide mock data for testing.
The mock data is based upon real data to make tests as realistic as possible

Functions:
- sample_daily_row_count_full_data
"""


import pytest
import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, BooleanType, FloatType, StringType, LongType, DoubleType, TimestampType, IntegerType

@pytest.fixture(scope="session")
def sample_daily_row_count_full_data(spark):
    
    test_data = spark.createDataFrame([['2024-02-05', 17],
                                            ['2024-02-04', 2301],
                                            ['2024-02-03', 1370],
                                            ['2024-02-02', 1266],
                                            ['2024-02-01', 1125],
                                            ['2024-01-31', 1604],
                                            ['2024-01-30', 1179],
                                            ['2024-01-29', 1163],
                                            ['2024-01-28', 1311],
                                            ['2024-01-27', 1508],
                                            ['2024-01-26', 1455],
                                            ['2024-01-25', 1554],
                                            ['2024-01-24', 878],
                                            ['2024-01-23', 745],
                                            ['2024-01-22', 577],
                                            ['2024-01-21', 716],
                                            ['2024-01-20', 792],
                                            ['2024-01-19', 787],
                                            ['2024-01-18', 826],
                                            ['2024-01-17', 826]]).cache()
    return test_data

@pytest.fixture(scope="session")
def sample_daily_row_count_missing_data(spark):
    test_data = spark.createDataFrame([['2024-02-05', 17],
                                            ['2024-02-04', 10],
                                            ['2024-02-03', 1370],
                                            ['2024-02-02', 1266],
                                            ['2024-02-01', 1125],
                                            ['2024-01-31', 1604],
                                            ['2024-01-30', 1179],
                                            ['2024-01-29', 1163],
                                            ['2024-01-28', 1311],
                                            ['2024-01-27', 1508],
                                            ['2024-01-26', 1455],
                                            ['2024-01-25', 1554],
                                            ['2024-01-24', 878],
                                            ['2024-01-23', 745],
                                            ['2024-01-22', 577],
                                            ['2024-01-21', 716],
                                            ['2024-01-20', 792],
                                            ['2024-01-19', 787],
                                            ['2024-01-18', 826],
                                            ['2024-01-17', 826]]).cache()
    return test_data
    
@pytest.fixture(scope="session")
def date_for_check(spark):
    return "2024-02-04"

@pytest.fixture(scope="session")
def sample_wallet_pos(spark):
    
    mock_list = [['045810384408240207', '94741316',     '4581',    '038',     '4408','2024-02-06',  'store', '2024-02-06 00:11:27', '2024-02-06 00:11:14',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240206', '91677913',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:29:01', '2024-02-05 16:28:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240224', '91677943',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:19:01', '2024-02-05 16:18:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240202', '91677963',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:09:01', '2024-02-05 16:08:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        [     '2342413000761','104569046',       '-1',     '-1',       '-1','9999-01-01',   'ecom', '2024-02-04 16:58:50', '2024-02-03 14:32:32',           'GB',        'ODS',  'EAGLE EYE','2024-02-05 03:38:...',     'svcgbse','2024-02-05 03:38:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        [     '2342436001532','101674822',       '-1',     '-1',       '-1','9999-01-01',   'ecom', '2024-02-03 16:02:08', '2024-02-03 13:47:36',           'GB',        'ODS',  'EAGLE EYE','2024-02-04 05:26:...',     'svcgbse','2024-02-04 05:26:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        [     '2342436001132','101676822',       '-1',     '-1',       '-1','9999-01-01',   'ecom', '2024-02-03 12:02:08', '2024-02-03 11:47:36',           'GB',        'ODS',  'EAGLE EYE','2024-02-04 05:26:...',     'svcgbse','2024-02-04 05:26:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['048230445320240203', '93284138',     '4823',    '044',     '5320','2024-02-02',  'store', '2024-02-02 10:26:45', '2024-02-02 10:26:29',           'GB',        'ODS',  'EAGLE EYE','2024-02-03 04:24:...',     'svcgbse','2024-02-03 04:24:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...']]
        
    mock_schema = StructType([StructField('trans_rcpt_nbr', StringType(), True),
                                StructField('wallet_id', StringType(), True),
                                StructField('store_nbr', StringType(), True),
                                StructField('reg_nbr', StringType(), True),
                                StructField('trans_nbr', StringType(), True),
                                StructField('visit_dt', StringType(), True),
                                StructField('chnl_nm', StringType(), True),
                                StructField('event_ts', StringType(), True),
                                StructField('src_create_ts', StringType(), True),
                                StructField('geo_region_cd', StringType(), True),
                                StructField('op_cmpny_cd', StringType(), True),
                                StructField('data_src_cd', StringType(), True),
                                StructField('load_ts', StringType(), True),
                                StructField('load_user_id', StringType(), True),
                                StructField('upd_ts', StringType(), True),
                                StructField('upd_user_id', StringType(), True),
                                StructField('md_process_id', StringType(), True),
                                StructField('md_source_ts', TimestampType(), True),
                                StructField('md_created_ts', TimestampType(), True),
                                StructField('md_source_path', StringType(), True)])

    mock_df = spark.createDataFrame(mock_list, schema=mock_schema).cache()
    return mock_df

@pytest.fixture(scope="session")
def sample_wallet_pos_with_dupes(spark):
    
    mock_list = [['045810384408240207', '94741316',     '4581',    '038',     '4408','2024-02-06',  'store', '2024-02-06 00:11:27', '2024-02-06 00:11:14',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240206', '91677913',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:29:01', '2024-02-05 16:28:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240206', '91677913',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:29:01', '2024-02-05 16:28:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240265', '91677913',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:29:01', '2024-02-05 16:28:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240224', '91677943',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:19:01', '2024-02-05 16:18:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['058280443999240202', '91677963',     '5828',    '044',     '3999','2024-02-05',  'store', '2024-02-05 16:09:01', '2024-02-05 16:08:47',           'GB',        'ODS',  'EAGLE EYE','2024-02-06 03:43:...',     'svcgbse','2024-02-06 03:43:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        [     '2342413000761','104569046',       '-1',     '-1',       '-1','9999-01-01',   'ecom', '2024-02-04 16:58:50', '2024-02-03 14:32:32',           'GB',        'ODS',  'EAGLE EYE','2024-02-05 03:38:...',     'svcgbse','2024-02-05 03:38:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        [     '2342436001532','101674822',       '-1',     '-1',       '-1','9999-01-01',   'ecom', '2024-02-03 16:02:08', '2024-02-03 13:47:36',           'GB',        'ODS',  'EAGLE EYE','2024-02-04 05:26:...',     'svcgbse','2024-02-04 05:26:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        [     '2342436001132','101676822',       '-1',     '-1',       '-1','9999-01-01',   'ecom', '2024-02-03 12:02:08', '2024-02-03 11:47:36',           'GB',        'ODS',  'EAGLE EYE','2024-02-04 05:26:...',     'svcgbse','2024-02-04 05:26:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...'],
                        ['048230445320240203', '93284138',     '4823',    '044',     '5320','2024-02-02',  'store', '2024-02-02 10:26:45', '2024-02-02 10:26:29',           'GB',        'ODS',  'EAGLE EYE','2024-02-03 04:24:...',     'svcgbse','2024-02-03 04:24:...',    'svcgbse','b0e9645c-64f7-47e...',datetime.datetime.strptime('2024-02-06 09:34:07', '%Y-%m-%d %H:%M:%S'),datetime.datetime.strptime('2024-02-06 09:47:10', '%Y-%m-%d %H:%M:%S'),'iddi/download/pro...']]
        
    mock_schema = StructType([StructField('trans_rcpt_nbr', StringType(), True),
                                StructField('wallet_id', StringType(), True),
                                StructField('store_nbr', StringType(), True),
                                StructField('reg_nbr', StringType(), True),
                                StructField('trans_nbr', StringType(), True),
                                StructField('visit_dt', StringType(), True),
                                StructField('chnl_nm', StringType(), True),
                                StructField('event_ts', StringType(), True),
                                StructField('src_create_ts', StringType(), True),
                                StructField('geo_region_cd', StringType(), True),
                                StructField('op_cmpny_cd', StringType(), True),
                                StructField('data_src_cd', StringType(), True),
                                StructField('load_ts', StringType(), True),
                                StructField('load_user_id', StringType(), True),
                                StructField('upd_ts', StringType(), True),
                                StructField('upd_user_id', StringType(), True),
                                StructField('md_process_id', StringType(), True),
                                StructField('md_source_ts', TimestampType(), True),
                                StructField('md_created_ts', TimestampType(), True),
                                StructField('md_source_path', StringType(), True)])

    mock_df = spark.createDataFrame(mock_list, schema=mock_schema)
    return mock_df


@pytest.fixture(scope='session')
def sample_historical_snapshot(spark):

    sample_list = [['min_daily_row_count'   ,'1.0'               , datetime.datetime.strptime('2024-02-07 11:36:17.852', '%Y-%m-%d %H:%M:%S.%f'), 1],
                    ['mean_daily_row_count'  ,'358389.61041902605', datetime.datetime.strptime('2024-02-07 11:35:42.613', '%Y-%m-%d %H:%M:%S.%f'), 1],
                    ['stddev_daily_row_count','301038.5958465282' , datetime.datetime.strptime('2024-02-07 11:36:37.799', '%Y-%m-%d %H:%M:%S.%f'), 1],
                    ['max_daily_row_count'   ,'1243939.0'      , datetime.datetime.strptime('2024-02-07 11:36:07.861', '%Y-%m-%d %H:%M:%S.%f'), 1],
                    ['total_row_count'       ,'316458026.0'       , datetime.datetime.strptime('2024-02-07 11:36:42.798', '%Y-%m-%d %H:%M:%S.%f'), 1]]
    
    sample_schema = StructType([StructField('history_type', StringType(), False),
                                StructField('historical_value', StringType(), True),
                                StructField('dts', TimestampType(), False),
                                StructField('row_number', IntegerType(), False)])

    sample_df = spark.createDataFrame(sample_list, sample_schema)
    return sample_df

