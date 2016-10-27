"""
 Using pyspark.streaming.StreamListerner for listening to streams.
 Internally it uses py4j framework to wrap and send it as a org.apache.spark.streaming.api.java.JavaStreamingListener object.
 View source at
 https://github.com/apache/spark/blob/master/streaming/src/main/scala/org/apache/spark/streaming/api/java/JavaStreamingListener.scala,
 https://github.com/apache/spark/blob/master/python/pyspark/streaming/listener.py (This Python class implements the Java interface
 org.apache.spark.streaming.api.java.PythonStreamingListener via py4j framework.)
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext, StreamingListener

class MyStreamingListener(StreamingListener):
    """
    Uses py4j framework to send Java objects to the pyspark process.
    The parameters to the callbacks are Java objects with members variables as objects.
    They are not sent as primitive data types.
    """
    def onBatchStarted(self, batchStarted):
        # 'batchStarted' instance of org.apache.spark.streaming.api.java.JavaStreamingListenerBatchStarted
        print('>>> Batch completed...number of records: ', batchStarted.batchInfo().numRecords())

    def onBatchCompleted(self, batchCompleted):
        # 'batchStarted' instance of org.apache.spark.streaming.api.java.JavaStreamingListenerBatchCompleted
        print('>>> Batch completed...time taken (ms) = ', batchCompleted.batchInfo().totalDelay())
        
if __name__ == '__main__':
    ssc = StreamingContext(\
        SparkContext(conf = SparkConf().setAppName('TestStreamingListenerJob')), \
        5)
 
    ssc.addStreamingListener(MyStreamingListener())
    
    ssc\
         .socketTextStream('localhost', 9999)\
         .flatMap(lambda line: line.split(' '))\
         .count()\
         .pprint()

    ssc.start()
    ssc.awaitTermination()
