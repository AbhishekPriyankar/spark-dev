## Apache Spark Development

### Using Python and Scala  

#### Compliant with Spark 2.0

Samples showing Scala and PySpark API usage in Scala and Python respectively.  

Also contains a framework for testing PySpark API using PyUnit.   

Subclass the PyUnit test case from the [PySparkTest](https://github.com/prithvirajbose/spark-dev/tree/master/src/main/python/pysparktest) class. The PySparkTest class encapsulates the [SparkSession](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession) which can be used as an entry point instead of [SparkContext](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.SparkContext).   

Example:

    from pysparktest import PySparkTest
    
    class SampleTest(PySparkTest):
    	def test_word_cnt(self):
        rdd = self.spark.sparkContext.parallelize(['Hi there', 'Hi'])
        self.assertEqual(word_cnt(rdd).collectAsMap(), {'Hi' : 2, 'there' : 1})
        

