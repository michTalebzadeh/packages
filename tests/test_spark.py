import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import HiveContext

@pytest.mark.spark
def test_spark_session():
    def spark_session():
        assert SparkSession.builder \
            .appName("test") \
            .enableHiveSupport() \
            .getOrCreate()

@pytest.mark.sc
def test_spark_context():
    assert SparkContext.getOrCreate()

@pytest.mark.hc
def test_hive_context():
    assert HiveContext(SparkContext.getOrCreate())