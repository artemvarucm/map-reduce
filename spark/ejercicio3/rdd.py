from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Ej3") \
    .getOrCreate()

sc = spark.sparkContext
rdd = spark.read.csv(sys.argv[1], header=True).rdd
rdd = rdd.map(lambda x: (x[0][:4], (float(x[4]), 1))) \
   
rdd = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
    .map(lambda x: (x[0], x[1][0] / x[1][1]))

rdd.saveAsTextFile(sys.argv[2])
