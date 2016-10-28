"""
    Shows how to join dataframes with PySpark 2.0 API
    as well as with vanilla SQL statements.  
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def getSparkSession():
    return SparkSession\
           .builder.appName('pyspark-join-job')\
           .config(conf = SparkConf())\
           .getOrCreate()

if __name__ == '__main__':

    spark = getSparkSession()
    
    phy = [(1, 'Jenny', 66), (2, 'Benny', 67), (3, 'Oscar', 46), (4, 'Shiva', 53)]
    chem = [(2, 'Benny', 70), (3, 'Oscar', 50)]

    phy_df = spark.createDataFrame(phy)
    phy_df = phy_df\
             .withColumnRenamed('_1', 'id')\
             .withColumnRenamed('_2', 'name')\
             .withColumnRenamed('_3', 'marks')

    chem_df = spark.createDataFrame(chem)
    chem_df = chem_df.withColumnRenamed('_1', 'id')\
              .withColumnRenamed('_2', 'name')\
              .withColumnRenamed('_3', 'marks')

    phy_df.printSchema()
    chem_df.printSchema()

    phy_df.agg(avg(phy_df.marks)).show()
    chem_df.groupBy().avg('marks').show() # alternate API

    #inner join
    phy_df\
            .join(chem_df, phy_df.id == chem_df.id)\
            .select(phy_df.name, phy_df.marks, chem_df.marks).show()

    #with SQL statements
    phy_df.createOrReplaceTempView('phy')
    chem_df.createOrReplaceTempView('chem')

    sql_str = 'select phy.name, phy.marks, chem.marks from phy, chem where phy.id = chem.id'
    spark.sql(sql_str).show()

    sql_str = 'select phy.name, phy.marks, chem.marks from phy full outer join chem on phy.id = chem.id'
    spark.sql(sql_str).show()

    '''
    Good practice to remove the view from memory.
    Scala API exists (org.apache.spark.sql.catalog.Catalog) but python API not expose.
    Check thin wrapper for Scala, pyspark.catalog.py file for details.
    '''
    spark.catalog.dropTempView('phy')
    spark.catalog.dropTempView('chem')

    
