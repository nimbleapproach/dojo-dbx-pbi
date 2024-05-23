# Databricks notebook source
# MAGIC %md
# MAGIC ## Evaluation of Tests
# MAGIC
# MAGIC This notebook is design to allow us to evaluate our data quality checks over historical data.
# MAGIC
# MAGIC It will provide a visual of how the chosen test metric changes over time, as well as a %of rejections over that time.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC In the cell below we can add any metrics we want to use to evaluate outliers

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import median_abs_deviation

#globla params:
core_catalog = 'coreprod'
catalog = 'custanwo'
schema = 'bi_data_model'



class Metric():
    """Base class for any metric to evaluate outliers based on
    Any metric should return a data time series representing the cuttoff value
    from the metric.
    """
    def __init__(self, window):
        self.window = window
        return None
    def calc_metric_over_window(self, data):
        return None


class PercentageOfMedian(Metric):
    """Evaluate outliers based on a percentage of the median row counts over a window"""
    def __init__(self, window:int, tolerance:float):
        """Initialise class
        
        Args:
            window(int): the window to take the median of
            tolerance(float): the acceptange fraction of the median
        Returns:
            None
        """
        self.window = window
        self.tolerance = tolerance
    def calc_metric_over_window(self,data: list) -> list:
        """ 
        Calculate the metric over the chosen window
        
        Args:
            data (list[int]): The row count data we will test based on
        Returns:
            final_list (list): the cutoff calculated based on the metric
        """
        # Convert array of integers to pandas series
        numbers_series = pd.Series(data)
        # Get the window of series of observations of specified window size
        windows = numbers_series.rolling(self.window)
        # Create a series of moving averages of each window
        moving_averages = windows.median() * self.tolerance
        # Convert pandas series back to list
        moving_averages_list = moving_averages.tolist()
        
        # Remove null entries from the list
        final_list = moving_averages_list[self.window - 1:]
        return final_list

class PercentageOfLowerQuartile(Metric):
    def __init__(self, window:int, tolerance:float):
        """Initialise class
        
        Args:
            window(int): the window to take the median of
            tolerance(float): the acceptange fraction of the median
        Returns:
            None
        """
        self.window = window
        self.tolerance = tolerance
    def calc_metric_over_window(self,data):
        """ 
        Calculate the metric over the chosen window
        
        Args:
            data (list[int]): The row count data we will test based on
        Returns:
            final_list (list): the cutoff calculated based on the metric
        """
        # Convert array of integers to pandas series
        numbers_series = pd.Series(data)
        # Get the window of series of observations of specified window size
        windows = numbers_series.rolling(self.window)
        # Create a series of moving averages of each window
        moving_averages = windows.quantile(0.25) * self.tolerance
        # Convert pandas series back to list
        moving_averages_list = moving_averages.tolist()
        
        # Remove null entries from the list
        final_list = moving_averages_list[self.window - 1:]
        return final_list

class PercentageOfMean(Metric):
    def __init__(self, window:int, tolerance:float):
        """Initialise class
        
        Args:
            window(int): the window to take the median of
            tolerance(float): the acceptange fraction of the median
        Returns:
            None
        """
        self.window = window
        self.tolerance = tolerance
    def calc_metric_over_window(self,data):
        """ 
        Calculate the metric over the chosen window
        
        Args:
            data (list[int]): The row count data we will test based on
        Returns:
            final_list (list): the cutoff calculated based on the metric
        """
        # Convert array of integers to pandas series
        numbers_series = pd.Series(data)
        # Get the window of series of observations of specified window size
        windows = numbers_series.rolling(self.window)
        # Create a series of moving averages of each window
        moving_averages = windows.mean() * self.tolerance
        # Convert pandas series back to list
        moving_averages_list = moving_averages.tolist()
        
        # Remove null entries from the list
        final_list = moving_averages_list[self.window - 1:]
        return final_list


class InterquartileRange(Metric):
    def __init__(self, window:int, iqr_scaling_factor:float):
        """Initialise class
        
        Args:
            window(int): the window to take the median of
            tolerance(float): the acceptange fraction of the median
        Returns:
            None
        """
        self.window = window
        self.iqr_scaling_factor = iqr_scaling_factor

    def calc_metric_over_window(self, data):
        """ 
        Calculate the metric over the chosen window
        
        Args:
            data (list[int]): The row count data we will test based on
        Returns:
            final_list (list): the cutoff calculated based on the metric
        """
        # Convert array of integers to pandas series
        numbers_series = pd.Series(data)
        # Get the window of series of observations of specified window size
        windows = numbers_series.rolling(self.window)
        # Create a series of moving averages of each window
        lower_q = windows.quantile(0.25)
        upper_q = windows.quantile(0.75)
        iqr = upper_q - lower_q

        moving_averages = windows.quantile(0.25) - (self.iqr_scaling_factor * iqr)
        # Convert pandas series back to list
        moving_averages_list = moving_averages.tolist()
        # Remove null entries from the list
        final_list = moving_averages_list[self.window - 1:]
        return final_list

class StandardDeviationFromMean(Metric):
    def __init__(self, window:int, standard_deviations:float):
        """Initialise class
        
        Args:
            window(int): the window to take the median of
            tolerance(float): the acceptange fraction of the median
        Returns:
            None
        """
        self.window = window
        self.standard_deviations = standard_deviations
        return None

    def calc_metric_over_window(self, data):
        """ 
        Calculate the metric over the chosen window
        
        Args:
            data (list[int]): The row count data we will test based on
        Returns:
            final_list (list): the cutoff calculated based on the metric
        """
        # Convert array of integers to pandas series
        numbers_series = pd.Series(data)
        # Get the window of series of observations of specified window size
        windows = numbers_series.rolling(self.window)
        # Create a series of moving averages of each window
        std = windows.std()

        moving_averages = windows.mean() - (self.standard_deviations * std)
        # Convert pandas series back to list
        moving_averages_list = moving_averages.tolist()
        # Remove null entries from the list
        final_list = moving_averages_list[self.window - 1:]
        return final_list

    
        

class RollingMetricCalculator():
    """Class which accepts the chosen metric and calculates the cutoff line"""
    def __init__(self, data: list, metric: Metric):
        """
        Initialise the class

        Args:
            data (list[int]): The row count data to test on
        Returns:
            metric (Metric): The evalution metric
        """
        self.metric = metric
        self.data = data
        return None
    
    def calc_cutoff(self):
        """Calculate the cutoff point
        
        Returns:
            cutoff (list): The cutoff value over each window
        """
        cuttoff = self.metric.calc_metric_over_window(self.data)
        return cuttoff


class MedianAbsoluteDeviation(Metric):
    def __init__(self, window:int, deviations:float):
        """Initialise class
        
        Args:
            window(int): the window to take the median of
            tolerance(float): the acceptange fraction of the median
        Returns:
            None
        """
        self.window = window
        self.deviations = deviations
        return None
    
    def calc_metric_over_window(self, data):
        """ 
        Calculate the metric over the chosen window
        
        Args:
            data (list[int]): The row count data we will test based on
        Returns:
            final_list (list): the cutoff calculated based on the metric
        """
        mads = []
        for i in range(len(data) - self.window):
            mads.append(np.median(data[i:i+self.window]) - (self.deviations * median_abs_deviation(data[i:i+self.window])))
            

        return mads


# COMMAND ----------

# MAGIC %md
# MAGIC #### Get row counts from SQL query:

# COMMAND ----------

#Sql query to get the row counts
total_testing_window = 450 # Default value
daily_count_df = spark.sql(f"""
                            SELECT CAST(event_ts AS date) AS dt,
                            count(*) AS row_count 
                            FROM {core_catalog}.gb_mb_dl_tables.mssn_wallets
                            WHERE CAST(event_ts AS date) > 
                                    DATE_ADD(CURRENT_DATE(), -{total_testing_window})
                            GROUP BY CAST(event_ts AS date)
                            ORDER BY CAST(event_ts AS date) DESC""")
        
data = [x[0] for x in daily_count_df.select('row_count').collect()]


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Test Evaluation:

# COMMAND ----------

#------------------------------------------------------
# Change metric parameters here
window_size = 40
metric = PercentageOfMedian(window_size, 0.4)
#------------------------------------------------------


RMC = RollingMetricCalculator(data, metric)
cutoff = RMC.calc_cutoff()

plt.xlabel('Days prior to today')
plt.ylabel('Row counts')
plt.title('distribution')
plt.plot(np.linspace(0,len(cutoff), len(cutoff)), data[window_size-1:], marker='.', ls='None')
plt.plot(np.linspace(0,len(cutoff), len(cutoff)), cutoff,label='cutoff line based on metric')
plt.legend()
plt.yscale('log') #Logarithmic axis can be helpful for really irrefular tables
plt.show()

rejected = (np.subtract(data[window_size-1:],cutoff)) < 0 
rejected_percent = sum(rejected)/len(rejected) 
print(f'{rejected_percent * 100} % of days were rejected with this measure')

# COMMAND ----------

data
