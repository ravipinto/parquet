from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

if __name__ == "__main__":
    sc = SparkContext(appName="c2p")
    sqlContext = SQLContext(sc)

    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
        StructField("col4", StringType(), True),
        StructField("col5", StringType(), True),
        StructField("col6", StringType(), True)])

    rdd = sc.textFile("input.csv").map(lambda line: line.split(","))
    df = sqlContext.createDataFrame(rdd, schema)
    df.write.parquet('input-parquet')
