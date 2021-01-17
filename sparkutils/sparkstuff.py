import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext, HiveContext
from src.config import config

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

def loadTableintoBQ(spark,dataset,table):
    try:
        house_df = spark.read. \
            format("bigquery"). \
            option("dataset", dataset). \
            option("table", table). \
            load()
        return house_df
    except:
        print(f"""Could not load from  table {table}, quitting""")
        sys.exit(1)

def writeTableToBQ(dataFrame,mode,dataset,table):
    try:
        dataFrame. \
            write. \
            format("bigquery"). \
            mode(mode). \
            option("dataset", dataset). \
            option("table", table). \
            save()
    except:
        print(f"""Could not save table {table}, quitting""")
        sys.exit(1)

