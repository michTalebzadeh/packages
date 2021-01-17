from misc import usedFunctions as uf
import conf.parameters as v
from sparkutils import sparkstuff as s
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window



def test_analyze_UKHousing():
    spark = s.spark_session('test')
    assert spark
    spark.sparkContext._conf.setAll(v.settings)
    sc = s.sparkcontext()
    assert sc
    # Get data from Hive table
    regionname = "Kensington and Chelsea"
    tableName = "ukhouseprices"
    fullyQualifiedTableName = v.DSDB + '.' + tableName
    assert tableName > ""
    start_date = "2010-01-01"
    end_date = "2020-01-01"
    lst = (spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");
    uf.println(lst)
    if (spark.sql(f"""SHOW TABLES IN {v.DSDB} like '{tableName}'""").count() == 1):
        tablestat = spark.sql(f"""ANALYZE TABLE {fullyQualifiedTableName} compute statistics""")
        assert tablestat
        rows = spark.sql(f"""SELECT COUNT(1) FROM {fullyQualifiedTableName}""").collect()[0][0]
        assert rows > 0
    else:
        print(f"""No such table {fullyQualifiedTableName}""")
        sys.exit(1)
    wSpecY = Window().partitionBy(F.date_format('datetaken', "yyyy"))
    assert wSpecY
    wSpecM = Window().partitionBy(F.date_format('datetaken', "yyyy"),
                                  F.date_format('datetaken', "MM"))  ## partion by Year and Month
    assert wSpecM
    house_df = spark.sql(f"""select * from {fullyQualifiedTableName} where regionname = '{regionname}'""")
    assert house_df
    df2 = house_df.filter(col("datetaken").between(f'{start_date}', f'{end_date}')). \
        select( \
        F.date_format('datetaken', 'yyyy').alias('Year') \
        , round(F.avg('averageprice').over(wSpecY)).alias('AVGPricePerYear') \
        , round(F.avg('flatprice').over(wSpecY)).alias('AVGFlatPricePerYear') \
        , round(F.avg('TerracedPrice').over(wSpecY)).alias('AVGTeraccedPricePerYear') \
        , round(F.avg('SemiDetachedPrice').over(wSpecY)).alias('AVGSemiDetachedPricePerYear') \
        , round(F.avg('DetachedPrice').over(wSpecY)).alias('AVGDetachedPricePerYear')). \
        distinct().orderBy('datetaken', asending=True)
    assert df2
    df2.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.yearlyhouseprices""")
    a = spark.sql(f"""SELECT COUNT(1) from {v.DSDB}.yearlyhouseprices""").collect()[0][0]
    assert a > 0
    wSpecP = Window().orderBy('year')
    assert wSpecP
    df_lag = df2.withColumn("prev_year_value", F.lag(df2['AVGPricePerYear']).over(wSpecP))
    assert df_lag
    result = df_lag.withColumn('percent_change', F.when(F.isnull(df2.AVGPricePerYear - df_lag.prev_year_value), 0). \
                               otherwise(
        F.round(((df2.AVGPricePerYear - df_lag.prev_year_value) * 100.) / df_lag.prev_year_value, 1)))
    assert result

    rs = result.select('Year', 'AVGPricePerYear', 'prev_year_value', 'percent_change')
    assert rs
    rs.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.percenthousepricechange""")
    b = spark.sql(f"""SELECT COUNT(1) from {v.DSDB}.percenthousepricechange""").collect()[0][0]
    assert b > 0
    df3 = house_df.filter(col("datetaken").between('2018-01-01', '2020-01-01')). \
        select( \
        col('datetaken')[1:7].alias('Year-Month') \
        , round(F.avg('averageprice').over(wSpecM)).alias('AVGPricePerMonth') \
        , round(F.avg('flatprice').over(wSpecM)).alias('AVGFlatPricePerMonth') \
        , round(F.avg('TerracedPrice').over(wSpecM)).alias('AVGTeraccedPricePerMonth') \
        , round(F.avg('SemiDetachedPrice').over(wSpecM)).alias('AVGSemiDetachedPricePerMonth') \
        , round(F.avg('DetachedPrice').over(wSpecM)).alias('AVGDetachedPricePerMonth')). \
        distinct().orderBy('datetaken', asending=True)
    assert df3
    df3.write.mode("overwrite").saveAsTable(f"""{v.DSDB}.monthlyhouseprices""")
    c = spark.sql(f"""SELECT COUNT(1) from {v.DSDB}.monthlyhouseprices""").collect()[0][0]
    assert c > 0

