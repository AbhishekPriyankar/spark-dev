"""
Converts tab separated fields to parquet format.
1 record = 6 fields
"""
import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def getSparkSession():
    return SparkSession.builder.appName('Parquet-Conv')\
           .config(conf = SparkConf()).getOrCreate()

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print('Usage: spark-submit [spark options] parquet_converter.py input_file output_file')
        sys.exit(-1)
        
    spark = getSparkSession()

    sch = StructType([\
        StructField('Date', StringType()),\
        StructField('Time', StringType()),\
        StructField('City', StringType()),\
        StructField('Category', StringType()),\
        StructField('Amount', DoubleType()),\
        StructField('Method', StringType()),\
        ])
    
    df = spark.read.csv(sys.argv[1], sep = '\t', mode = 'DROPMALFORMED', schema = sch)
    df.write.parquet(sys.argv[2], mode = 'overwrite')
    print('>>> Records written: ', df.count())
              
    
