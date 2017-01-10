## Apache Spark Development

### Using Python and Scala  

#### Compliant with Spark 2.0

Samples showing Scala and PySpark API usage in Scala and Python respectively.  

Also contains a base class for testing PySpark code using SparkSession and PyUnit.   

This base class is a slight changed version from the [PySparkTestCase](https://github.com/apache/spark/blob/master/python/pyspark/streaming/tests.py) class in the pyspark.test module.   
Subclass the PyUnit test case from the [CustomPySparkTestCase](https://github.com/prithvirajbose/spark-dev/tree/master/src/main/python/pysparktest) class. The CustomPySparkTestCase class encapsulates the [SparkSession](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession) which can be used as an entry point instead of [SparkContext](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext).   

Example:

    from pysparktest import CustomPySparkTestCase
    
    class SampleTest(CustomPySparkTestCase):
    	def test_word_cnt(self):
        rdd = self.spark.sparkContext.parallelize(['Hi there', 'Hi'])
        self.assertEqual(word_cnt(rdd).collectAsMap(), {'Hi' : 2, 'there' : 1})
        

