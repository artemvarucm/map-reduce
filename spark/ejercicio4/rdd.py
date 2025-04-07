from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Ej5") \
    .getOrCreate()

sc = spark.sparkContext
rdd = spark.read.csv(sys.argv[1], header=True, sep=";").rdd
rdd = rdd.filter(lambda x: x[4] != None)
rdd = rdd.map(lambda x: (x[3], (float(x[4]), 1)))
rdd = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
    .map(lambda x: (x[0], x[1][0] / x[1][1]))

rdd.saveAsTextFile(sys.argv[2])
