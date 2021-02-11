import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from src.config import config

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
        spark.conf.set("hive.metastore.authorization.storage.checks", "/usr/hive/warehouse")
        spark.conf.set("hive.stats.autogather", "true")
        spark.conf.set("hive.metastore.disallow.incompatible.col.type.changes", "false")
        spark.conf.set("set hive.resultset.use.unique.column.names", "false")
        spark.conf.set("hive.metastore.uris", "thrift://rhes75:9083")
        return spark
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def setSparkConfBQ(spark):
    try:
        spark.conf.set("GcpJsonKeyFile", config['GCPVariables']['jsonKeyFile'])
        spark.conf.set("BigQueryProjectId", config['GCPVariables']['projectId'])
        spark.conf.set("BigQueryDatasetLocation", config['GCPVariables']['datasetLocation'])
        spark.conf.set("google.cloud.auth.service.account.enable", "true")
        spark.conf.set("fs.gs.project.id", config['GCPVariables']['projectId'])
        spark.conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        spark.conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        spark.conf.set("temporaryGcsBucket", config['GCPVariables']['tmp_bucket'])
        return spark
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def loadTableFromBQ(spark,dataset,tableName):
    try:
        house_df = spark.read. \
            format("bigquery"). \
            option("dataset", dataset). \
            option("table", tableName). \
            load()
        return house_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def writeTableToBQ(dataFrame,mode,dataset,tableName):
    try:
        dataFrame. \
            write. \
            format("bigquery"). \
            mode(mode). \
            option("dataset", dataset). \
            option("table", tableName). \
            save()
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def loadTableFromJDBC(spark, url, tableName, user, password, driver, fetchsize):
    try:
       house_df = spark.read. \
            format("jdbc"). \
            option("url", url). \
            option("dbtable", tableName). \
            option("user", user). \
            option("password", password). \
            option("driver", driver). \
            option("fetchsize", fetchsize). \
            load()
       return house_df
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)


def writeTableWithJDBC(dataFrame, url, tableName, user, password, driver, mode):
    try:
        dataFrame. \
            write. \
            format("jdbc"). \
            option("url", url). \
            option("dbtable", tableName). \
            option("user", user). \
            option("password", password). \
            option("driver", driver). \
            mode(mode). \
            save()
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)
