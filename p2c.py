from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    sc = SparkContext(appName="p2c")
    sqlContext = SQLContext(sc)

    readdf = sqlContext.read.parquet('input-parquet')
    readdf.rdd.map(tuple).map(lambda row: str(row[0]) +
                              "," + str(row[1]) + "," + str(row[2]) + "," + str(row[3]) + "," +
                              str(row[4])+"," + str(row[5])).saveAsTextFile("output.csv")
