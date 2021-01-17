from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
import sys
#import findspark
#findspark.init()

def spark_session(appName):
  return SparkSession.builder \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

def sparkcontext():
  return SparkContext.getOrCreate()

def hivecontext():
  return HiveContext(sparkcontext())

def spark_session_local(appName):
    return SparkSession.builder \
        .master('local[1]') \
        .appName(appName) \
        .enableHiveSupport() \
        .getOrCreate()

def setSparkConfHive(spark):
    try:
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark.conf.set("spark.sql.orc.filterPushdown", "true")
        spark.conf.set("hive.msck.path.validation", "ignore")
        spark.conf.set("hive.metastore.authorization.storage.checks", "false")
        spark.conf.set("hive.metastore.client.connect.retry.delay", "5s")
        spark.conf.set("hive.metastore.client.socket.timeout", "1800s")
        spark.conf.set("hive.metastore.connect.retries", "12")
        spark.conf.set("hive.metastore.execute.setugi", "false")
        spark.conf.set("hive.metastore.failure.retries", "12")
        spark.conf.set("hive.metastore.schema.verification", "false")
        spark.conf.set("hive.metastore.schema.verification.record.version", "false")
        spark.conf.set("hive.metastore.server.max.threads", "100000")
        spark.conf.set("hive.metastore.authorization.storage.checks", "/apps/hive/warehouse")
        spark.conf.set("hive.stats.autogather", "true")
        return spark
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)
