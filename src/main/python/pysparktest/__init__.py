"""
Base class for PySpark tests.
Encapsulates the creation of the SparkSession in setUpClass
and stops the same in tearDownClass.

Developed on Apache Spark 2.x
"""
__version__ = "1.0"
__author__ = 'Prithviraj Bose'
__license__ = "BSD"
__email__ = "prithvirajbose@hotmail.com"

__all__ = ['PySparkTest']
