"""
Test code for parquet formats
1. Perform group functions -> aggregate, max in a group.
"""

import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

def getSparkSession():
    return SparkSession.builder.appName('Parquet-Test')\
           .config(conf = SparkConf()).getOrCreate()

if __name__ == '__main__':

    if (len(sys.argv) != 2):
        print('Usage: spark-submit [spark options] parquet_test.py parquet_file')
        sys.exit(-1)

    spark = getSparkSession()
        
    df = spark.read.parquet(sys.argv[1])

    df.cache()

    df.printSchema()

    print('Number of records: ', df.count())

    print('Number of Method=\'Discover\': ', df.filter(df.Method == 'Discover').count())

    '''
    Can be written like this as well,

    >>> from pyspark.sql.functions import sum
    >>> for row in df.groupBy(df.Method).agg(sum(df.Amount))\
            .withColumnRenamed('sum(Amount)', 'Total')\
            .orderBy('Total', ascending=False)\
            .take(3):
            print(row)
    '''
    print('>>> Top 3 Methods using Dataframe API >>>')
    for row in df.groupBy(df.Method).sum()\
        .withColumnRenamed('sum(Amount)', 'Total')\
        .orderBy(desc('Total'))\
        .take(3):
        print(row)

    print('>>> Top 3 Methods using SQL >>>')
    df.createOrReplaceTempView('temp')
    sql_str = 'select Method, sum(Amount) as Total from temp group by Method order by Total desc'
    for row in spark.sql(sql_str).take(3):
        print(row)

    # code for cleaning up memory...
    spark.catalog.dropTempView("temp")
    df.unpersist()
        
