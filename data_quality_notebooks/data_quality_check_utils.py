import logging
import datetime
import pyspark.sql.functions as F
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType, FloatType, StringType
from typing import Optional, Tuple, Protocol
from logging import Logger

#-------------------------------------------------------------------------------
#Implementation of different test metrics, Add a child class to add a new metric
class CutoffMetric(Protocol):
    """
    Base class for implementing different cutoff metric calculations.

    Methods:
    - set_params(input_dataframe: DataFrame, tolerance: float): Sets the input dataframe and tolerance for the metric.
    - calc_cutoff() -> DataFrame: Calculates the cutoff using the specified metric and returns the result as a DataFrame.
    """
    def set_params(self, input_dataframe: DataFrame, tolerance: float):
        """Sets the input dataframe and tolerance for the metric."""
        raise NotImplementedError()

    def calc_cutoff(self) -> DataFrame:
        """Calculates the cutoff using the specified metric and returns the result as a DataFrame."""
        raise NotImplementedError()


class PercentageOfMedian(CutoffMetric):
    """
    A cutoff metric implementation based on the percentage of the median.

    Methods:
    - set_params(input_dataframe: DataFrame, tolerance: float): Sets the input dataframe and tolerance for the metric.
    - calc_cutoff() -> DataFrame: Calculates the cutoff using the percentage of median and returns the result as a DataFrame.
    """
    def set_params(self, input_dataframe: DataFrame, tolerance: float):
        """Sets the input dataframe and tolerance for the metric."""
        self.input_dataframe = input_dataframe
        self.tolerance = tolerance

        if self.input_dataframe.count() == 0:
            raise Exception("Dataframe for calculating cutoff is empty.")
        
    def calc_cutoff(self) -> DataFrame:
        """Calculates the cutoff using the percentage of median and returns the result as a DataFrame."""
        
        aggregation_column = self.input_dataframe.columns[1]
        
        # Define 'low counts' according to median and tolerance
        input_dataframe = (self.input_dataframe
                            .withColumn("low_count_cutoff",
                            F.lit(self.input_dataframe
                                .select(F.median(aggregation_column))
                                .collect()[0][f'median({aggregation_column})'] * self.tolerance)))
        
        # Run check on daily count
        input_dataframe = (input_dataframe
                            .withColumn("pass_check",
                                F.when((F.col(aggregation_column) < input_dataframe.low_count_cutoff), False)
                                        .otherwise(True))).cache()
        
        return input_dataframe

#-------------------------------------------------------------------------------
#Initialise logger for debugging:
def set_up_logger(level:str) -> Logger:
    """Set up logger and return for use"""
    logging.basicConfig(format=('%(asctime)s: '
                                '%(filename)s: '    
                                '%(levelname)s: '
                                '%(funcName)s(): '
                                '%(lineno)d:\t'
                                '%(message)s'))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.getLevelName(level))
    return logger

#-------------------------------------------------------------------------------
#Daily count test and ata load functions

def load_daily_counts(query:str, spark: SparkSession) -> Tuple[Optional[DataFrame], Optional[str]]:
    """
    Load the daily counts from the source table
    Args:
        query (string): The query to get daily counts
        spark (SparkSession): Spark session to use from notebook
    Returns:
        daily_count_df Optional(bool): The daily row counts
        err_msg Optional(str): Contains any error in query.
    
    """
    try:
        #get counts over specified window
        daily_count_df = spark.sql(query).cache()
        
        return daily_count_df, None
    except Exception as e:
        err_msg = e
        return None, str(err_msg)
    

def compare_daily_counts(daily_count_df: DataFrame, metric: CutoffMetric, tolerance: float, date_for_check: str) -> bool:
    """
    Test the daily count value against the designated cutoff

    Args:
        daily_count_df (DataFrame): The daily row counts for the selected table
        tolerance (float): The value to scale the count cutoff
    Returns:
        daily_check_result (bool): True if check passes, False otherwise
    """

    dt_column = daily_count_df.columns[0]
    
    metric.set_params(daily_count_df, tolerance)
    daily_count_df = metric.calc_cutoff()

    #Display results
    (daily_count_df.cache()).show()
    #Return test result for most recent date. Return False if no record for today.
    if daily_count_df.filter(F.col(dt_column) == date_for_check).count() == 0:
        daily_count_check_result = False
    else:
        daily_count_check_result = (daily_count_df
                                .filter(F.col(dt_column) == date_for_check)
                                .select('pass_check').collect()[0][0]) 
    return daily_count_check_result


def run_daily_count_test(query:str,
                         metric: CutoffMetric,
                         date_for_check: str,
                         tolerance: float,
                         spark: SparkSession
                         ) -> Tuple[bool, Optional[str]]:
    """
    Function which executes all the steps required to run a test against the daily counts
    The query should contain a date column followed by a row count column

    Args:
        query (str): The query to get daily aggregations
        metric (CutoffMetric): The metric to calculate the low count cutoff with
        date_for_check (str): The date to conduct the test for
        tolerance (float): The value to scale the cutoff of the test.
        spark (SparkSession): Spark session to use from notebook
    Returns:
        daily_count_check_result: True if test has passed, False otherwise
        error_message (Optional[str]): Any error associated with the query
    """
    daily_count_df, error_message = load_daily_counts(query = query,spark = spark)
    if error_message is None and daily_count_df is not None:
        #run daily row count test
        daily_count_check_result = compare_daily_counts(daily_count_df = daily_count_df, 
                                                    metric = metric, 
                                                    tolerance=tolerance,
                                                    date_for_check = date_for_check)
        print("Daily count test result: {0}".format(daily_count_check_result))
    else:
        #Handle error:
        print("ERROR in daily count data load:")
        print(error_message)
        daily_count_check_result = False
    
    return daily_count_check_result, error_message


#-------------------------------------------------------------------------------
#DUplicate data test:
def run_duplicate_data_test(query: str,
                            tolerance: float,
                            spark: SparkSession
                      ) -> Tuple[bool, Optional[str]]:
    """
    Load and test for duplicate data in the source table
    Query should contain two columns that we expect to be equal 
    Args:
        query (str): The query to test for duplicates
        tolerance (float): The factor to adjust sensitivity.
        spark (SparkSession): spark session from notebook.
    Returns:
        daily_count_check_result (bool): True if row count meets critera, 
                                         False otherwise.
        err_msg Optional(str): Contains any error in query.
    
    """
    err_msg=None
    try:
        #get counts over specified window
        
        count_df = spark.sql(query).cache()
        
        count_column_1 = count_df.columns[1]
        count_column_2 = count_df.columns[2]
    
        #Get ratio of row_count:trans_rcpt_nbrs - if this is >1 then duplicate data
        count_df = count_df.withColumn("duplication_ratio",
                                                    F.col(count_column_1)/F.col(count_column_2))
        #define low counts based on the median
        count_df = (count_df
                            .withColumn("high_count_cuttoff",F.lit(1 + tolerance)))
        #Flag  based on cutoff defined above
        count_df = (count_df
                          .withColumn("pass_check",
                                F.when((count_df.duplication_ratio >
                                        count_df.high_count_cuttoff), False)
                                        .otherwise(True)))
        #Display results
        (count_df.cache()).show()

        duplicate_value_check_result = np.all([row['pass_check'] for row in count_df.select('pass_check').collect()])


        if duplicate_value_check_result == False:
            print("Duplicate data found on following days:")
            count_df.filter("pass_check == False").show()


        print("Duplicate data test result: {0}".format(duplicate_value_check_result))
        return bool(duplicate_value_check_result), err_msg
    except Exception as e:
        err_msg = e
        return False, str(err_msg)


#-------------------------------------------------------------------------------
#Historical test and data load functions:

def get_historic_snapshot(check_id: int, catalog:str, schema:str, spark: SparkSession) -> Optional[DataFrame]:
    """
    Gets any historical values associated with this test. If no values are recorded then None is returned.

    Args:
        check_id (int): Id corresponding to current check
        spark (SparkSession): Spark session to use from notebook
    Returns:
        Optional(DataFrame): If records exist, they are returned, otherwise None is returned

    """
    historical_snapshot_df = spark.sql(f"""
                            WITH cte AS (
                            SELECT history_type, historical_value, dts ,
                            ROW_NUMBER() OVER (PARTITION BY history_type 
                            ORDER BY dts DESC)
                            AS row_number
                            FROM {catalog}.{schema}.data_quality_check_historical_snapshot
                            where check_id = {check_id} )

                            SELECT * FROM cte 
                            WHERE row_number = 1
                                       """).cache()
    if historical_snapshot_df.count() == 0:
        print("No historical data found")
        return None
    else:
        return historical_snapshot_df
    

def parse_historic_snapshot(historical_snapshot_df :DataFrame) -> Tuple[dict, str]:
    """
    Parse historical values from dataframe into dictionary.
    Also returns the date to run the historical test from.

    Args:
        historical_snapshot_df(DataFrame): Data to be used in historical comparison
    Returns:
        result_dict (dict): Historical snapshot values organised by key
        date (str): The date to run the historical test prior to
    
    """
    history_type_keys = [x[0] for x in historical_snapshot_df.collect()]

    result_dict = {}
    for key in history_type_keys:
        result_dict[key] = float(historical_snapshot_df
                                        .filter(f"history_type = '{key}'")
                                        .select("historical_value")
                                        .collect()[0][0])
    #Get date snapshot was recorded on
    date = historical_snapshot_df.select("dts").collect()[0][0]
    # Date included in test will be day before:
    date = (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    return result_dict, date


def is_value_within_tolerance(test_value:float|int,
                              target_value: float|int,
                              tolerance: float
                              ) -> bool:
    """
    Function to return true if value is within target window, false otherwise
    Args:
        test_value (float|int): The value to be tested
        target_value (float|int): The target for the test value
        tolerance (float): The tolerage for the target window
    Returns:
        (bool): True if in window, False otherwise 
    
    """
    lower_bound = target_value * (1 - tolerance)
    upper_bound = target_value * (1 + tolerance)

    return ((test_value > lower_bound) and (test_value < upper_bound))

def compare_historic_counts(current_values_dict: dict,
                         historic_values_dict: dict,
                         tolerance: float,
                         spark: SparkSession
                         ) -> bool:
    """
    Compare historic values, and current values

    Args:
        current_values_dict (dict): values obtained from query
        historic_values_dict (dict): values saved in table
        tolerance (float): sensitivity of test
        spark (SparkSession): Spark session to use from notebook
    Returns
        aggregate_value_check_result (bool): True if values are within tolerance
    Raises:
        (Exception): If dictionary keys are not the same, an error is raised.
    """

    #First test that dictionary contains the same keys:
    if current_values_dict.keys() != historic_values_dict.keys():
        raise Exception("Current value dictionary and historic value dictionary contain different keys. They are not suitable for comparison.")

    value_comparison_list = [[x,
                            historic_values_dict[x],
                            current_values_dict[x], 
                            is_value_within_tolerance(test_value = current_values_dict[x], 
                                                      target_value = historic_values_dict[x],
                                                      tolerance = tolerance)]
                            for x in list(historic_values_dict.keys())]
    
    df_schema = StructType([StructField("history_type", StringType(), False),
                    StructField("stored_historic_value", FloatType(), False),
                    StructField("current_query_value", FloatType(), False),
                    StructField("pass_check", BooleanType(), False)])
    
    test_result_df = spark.createDataFrame(value_comparison_list, df_schema)
    #display results
    (test_result_df).show()
    #get final result - Logical 'AND' of the 'pass_check' column
    aggregate_value_check_result = np.all([row['pass_check'] for row in test_result_df.select('pass_check').collect()])

    return bool(aggregate_value_check_result)

def get_current_aggregations(query:str, date_for_check: str, spark: SparkSession) -> dict:
    """
    Gets hitoric aggregations for all dates before and including the date passed as arg.

    Args:
        date_for_check (str): All dates before and including this are aggregated
    Returns:
        result_row (dict): Dictionary of all aggregations and values
    """
    cte_df = spark.sql(query.format(date = date_for_check)).cache()
    aggregation = cte_df.columns[1]

    # Perform aggregations 
    result_row = {'mean_daily_row_count': float(cte_df.agg(F.mean(aggregation)).collect()[0][0]),
                'max_daily_row_count': float(cte_df.agg(F.max(aggregation)).collect()[0][0]),
                'min_daily_row_count': float(cte_df.agg(F.min(aggregation)).collect()[0][0]),
                'stddev_daily_row_count': float(cte_df.agg(F.stddev(aggregation)).collect()[0][0]),
                'total_row_count': float(cte_df.agg(F.sum(aggregation)).collect()[0][0])}
    return result_row

def save_current_aggregations(check_id: int,
                              catalog: str,
                              schema: str,
                              aggregations: dict,
                              spark: SparkSession) -> None:
    """
    Write current aggregations to history table

    Args:
        check_id (int): Id for current check
        aggregations (dict): The current aggregations from source table
        spark (SparkSession): The spark session from notebook
    Returns:
        None
    
    """
    for key in aggregations.keys():
        value = aggregations[key]
        spark.sql(f"""
            INSERT INTO {catalog}.{schema}.data_quality_check_historical_snapshot
            (check_id, history_type, historical_value)
            VALUES ({check_id}, '{key}', '{value}')
            """)
    return None

def run_historic_test(check_id: int,
                      date_for_check: str,
                      catalog: str,
                      schema: str,
                      historical_tolerance:float,
                      historical_query: str,
                      spark: SparkSession
                      ) -> Tuple[bool, Optional[str]]:
    """
    Run history test. If the test passes, then save new history values to test against.

    Args:
        check_id (int): Id for current check
        date_for_check (str): date for the current tests
        catalog (str): UC catalog for saving historic aggregations
        schema (str): UC schema for saving historic aggregations
        historical_tolerance (float): Sensitivity for the test
        historical_query (str): The query to get historic aggregations
        spark (SparkSession): The spark session from notebook
    Returns:
        history_test_result (bool): True if test has passed, False otherwise
        error_message (Optional[str]): Details of any error
    """

    error_message = None
    historic_snapshot_df = get_historic_snapshot(check_id = check_id,
                                                 catalog = catalog,
                                                 schema= schema,
                                                 spark = spark)

    if historic_snapshot_df is None:
        # No historic data is available:
        # Write new historic data:
        new_snapshot_dict = get_current_aggregations(query = historical_query,
                                                     date_for_check = date_for_check,
                                                     spark = spark)
        save_current_aggregations(check_id = check_id,
                                      catalog = catalog,
                                      schema = schema,
                                      aggregations = new_snapshot_dict,
                                      spark = spark)
        # Pass test but record error message:
        history_test_result = True
        error_message = "There was no historical data recorded, so historical test has passed."
    else:
        # We have historic data, so compare it to values from a query
        historic_values_dict, date_for_historic_comparison =  parse_historic_snapshot(historic_snapshot_df)
        queried_values_dict = get_current_aggregations(query = historical_query,
                                                       date_for_check = date_for_historic_comparison,
                                                       spark =spark)
        # Compare:
        history_test_result = compare_historic_counts(current_values_dict = queried_values_dict,
                                                   historic_values_dict = historic_values_dict,
                                                   tolerance = historical_tolerance,
                                                   spark = spark)
        
        if history_test_result == True:
            # Save new data for future historical comparison.
            new_snapshot_dict = get_current_aggregations(query = historical_query,
                                                         date_for_check = date_for_check,
                                                         spark = spark)
            save_current_aggregations(check_id = check_id,
                                      catalog = catalog,
                                      schema = schema,
                                      aggregations = new_snapshot_dict,
                                      spark = spark)
    
    print("Historic test result: {0}".format(history_test_result))
    return history_test_result, error_message

# ----------------------------------------
# General functions for testing date column
# Added for Datamart report tests

def date_column_check(date_column_query: str,
                     test_date: str,
                     spark: SparkSession) -> bool:
    """
    Test whether the specified date is in the table
    Query must produce a dt column.

    Args:
        date_column_query: The query to produce a date column to test
        test_date: The date that should be contained within the table
        spark: SparkSession required to run pyspark queries

    Returns:
        test_result: The result of the test. True if the date is in the table

    Raises:
        Exception: Error if dt column not in query
    """

    # Make sure 'dt' column is in table
    date_df = spark.sql(date_column_query).cache()
    date_df.show(truncate=False)
    if "dt" not in date_df.columns:
        raise Exception("Column 'dt' is not in the query provided. 'dt' column is required for date test.")

    most_recent_date = date_df.agg(F.max('dt')).collect()[0][0] # most recent date in datetime object
    test_date_obj = datetime.datetime.strptime(test_date, '%Y-%m-%d').date()

    print('Most recent date in table is: {0}'.format(most_recent_date.strftime('%Y-%m-%d')))

    if most_recent_date == test_date_obj:
        return True
    else:
        return False






