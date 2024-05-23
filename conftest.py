from pyspark.sql import SparkSession
import pytest


import sys
sys.path.append('../')
from tests.mock_data import *


@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder \
                    .appName('unit_tests') \
                    .getOrCreate()
    yield spark

