
import sys
import unittest

sys.path.insert(1, '../../src/main/python/pysparktest');

from pysparktest import PySparkTest

def word_cnt(rdd):
    return rdd.flatMap(lambda word: word.split(' '))\
           .map(lambda word: (word, 1))\
           .reduceByKey(lambda acc, n: acc + n)

class SampleTest(PySparkTest):
    def test_word_cnt(self):
        rdd = self.spark.sparkContext.parallelize(['Hi there', 'Hi'])
        self.assertEqual(word_cnt(rdd).collectAsMap(),\
                         {'Hi' : 2, 'there' : 1})

if __name__ == '__main__':
    unittest.main(verbosity = 2)

