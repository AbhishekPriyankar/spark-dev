"""
Testing the DataFrame API
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def getSparkSession():
    return SparkSession.builder.appName('API-Test')\
           .config(conf = SparkConf()).getOrCreate()

if __name__ == '__main__':

    spark = getSparkSession()
    data = [('Benny', 86), ('Jenny', 77), ('Oscar', 55), ('Scarlett', 89)]
    df = spark.createDataFrame(data)
    df = df.withColumnRenamed('_1', 'name').withColumnRenamed('_2', 'marks')

    print('Average marks: ', df.select(avg(df.marks)).collect())

    print('Marks between 80 & 90: ', df.filter(df.marks.between(80, 90)).collect())

    print('Marks between 80 & 90 and name starts with \'S\': ', \
          df.filter(df.marks.between(80, 90) & df.name.startswith('S')).collect())
    
    print('Names having \'y\': ', df.filter(df.name.like('%y%')).collect())

    names_with_y = df.filter(df.name.like('%y%'))
    
    print('Avg. of names having \'y\': ', \
          names_with_y.select(avg(names_with_y.marks)).collect())
    
