"""
 Shows usage of PySparkTestCase (Apache Spark's unit test base class)
"""
import os
import sys
import unittest

# add pyspark and py4j libs to the PYTHONPATH
spark_home = os.environ['SPARK_HOME']
sys.path.insert(1, '/media/linux-1/spark-2.0.0-bin-hadoop2.7/python');
sys.path.insert(1, os.path.join(spark_home, 'python/lib/pyspark.zip'))
sys.path.insert(1, os.path.join(spark_home, 'python/lib/py4j-0.10.1-src.zip'))

from pyspark.tests import ReusedPySparkTestCase

def word_cnt(rdd):
    return rdd.flatMap(lambda word: word.split(' '))\
           .map(lambda word: (word, 1))\
           .reduceByKey(lambda acc, n: acc + n)

class SampleTestWithPySparkTestCase(ReusedPySparkTestCase):
    def test_word_cnt(self):
        rdd = self.sc.parallelize(['Hi there', 'Hi'])
        self.assertEqual(word_cnt(rdd).collectAsMap(),\
                         {'Hi' : 2, 'there' : 1})

if __name__ == '__main__':
    unittest.main(verbosity = 2)

