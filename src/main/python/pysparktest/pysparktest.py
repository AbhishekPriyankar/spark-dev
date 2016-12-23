
import os
import sys

# add pyspark and py4j libs to the PYTHONPATH
spark_home = os.environ['SPARK_HOME']
sys.path.insert(1, os.path.join(spark_home, 'python/lib/pyspark.zip'))
sys.path.insert(1, os.path.join(spark_home, 'python/lib/py4j-0.10.1-src.zip'))

from pyspark import SparkConf
from pyspark.sql import SparkSession
import unittest

class PySparkTest(unittest.TestCase):
        """
        Class variables:
        spark - pyspark.sql.SparkSession
        """

        @classmethod
        def setUpClass(cls):
                cls.spark = SparkSession.builder\
                        .appName('tests')\
                        .master('local[*]')\
                        .config(conf = SparkConf())\
                        .getOrCreate()
                
        @classmethod
        def tearDownClass(cls):
                cls.spark.stop()

if __name__ == '__main__':
        unittest.main(verbosity = 2)

