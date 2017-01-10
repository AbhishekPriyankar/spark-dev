"""
Unit tests written using custom class CustomPySparkTestCase that encapsulates 
SparkSession.
"""

import sys
import unittest

sys.path.insert(1, '/media/linux-1/spark-dev/src/main/python/pysparktest')

from pyspark.sql.functions import avg

from pysparktest import CustomPySparkTestCase

class SQLTestsWithCustomPySparkTestCase(CustomPySparkTestCase):
    def test_avg(self):
        data = [('Benny', 86), ('Jenny', 77), ('Oscar', 55), ('Scarlett', 89)]
        df = self.spark.createDataFrame(data)
        df = df.withColumnRenamed('_1', 'name').withColumnRenamed('_2', 'marks')
        
        # data is returned as a list of tuples
        self.assertEqual(76.75, df.select(avg(df.marks)).collect()[0][0])

    def test_filter(self):
        data = [('Benny', 86), ('Jenny', 77), ('Oscar', 55), ('Scarlett', 89)]
        df = self.spark.createDataFrame(data)
        df = df.withColumnRenamed('_1', 'name').withColumnRenamed('_2', 'marks')
        
        self.assertEqual('Scarlett', df.filter(df.name.startswith('S')).collect()[0][0])
        
if __name__ == '__main__':
    unittest.main(verbosity = 2)