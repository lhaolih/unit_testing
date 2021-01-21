import pytest
from pyspark.sql import SparkSession
import logging


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope='session')
def spark(request):
    '''
      spark session for all the tests in test_main.py
    '''
    # request.addfinalizer(lambda: spark.stop())
    spark = SparkSession \
    .builder \
    .master('local[2]')\
    .appName("pytest_unit_test") \
    .getOrCreate()
    quiet_py4j()
    print(spark)
    return spark
    