
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, Row

if __name__ == '__main__':
    spark = SparkSession.builder.appName('ds_test')\
            .config(conf = SparkConf()).getOrCreate()

    parFile = os.path.join(os.environ['SPARK_HOME'], \
                           'examples/src/main/resources/users.parquet')
    
    usersDF = spark.read.parquet(parFile)

    usersDF.printSchema()
    usersDF.show()

    usersDF.filter(usersDF.favorite_color != 'null').show()
