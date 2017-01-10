"""
Design pattern for routing stream processing results.
"""

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

def processPartition(iterator):
    """
    This function routes the stream processing results
    """
    for item in iterator:
        print(item)

if __name__ == '__main__':
    ssc = StreamingContext(\
                           SparkContext(conf = SparkConf()\
                                        .setAppName('dstream_rdd_job')\
                                        .setMaster('local[*]')), 
                           5)
    
    receiver = ssc.socketTextStream('localhost', 9999)
    
    word_tuple = receiver.flatMap(lambda line: line.split(' '))\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda x, y: x + y)
    
    word_tuple.foreachRDD(lambda rdd: rdd.foreachPartition(processPartition))
    
    ssc.start()
    ssc.awaitTermination()
    