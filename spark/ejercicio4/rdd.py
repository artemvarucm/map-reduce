from pyspark.sql import SparkSession
import sys

def mapRatingToRange(rating):
    if rating <= 1:
        return "Range-1"
    elif rating <= 2:
        return "Range-2"
    elif rating <= 3:
        return "Range-3"
    elif rating <= 4:
        return "Range-4"
    else:
        return "Range-5"

spark = SparkSession.builder \
    .appName("Ej4") \
    .getOrCreate()

sc = spark.sparkContext
rdd = spark.read.csv(sys.argv[1], header=True, sep=",").rdd
rdd = rdd.map(lambda x: (x[1], (float(x[2]), 1)))
rdd = rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
    .map(lambda x: (x[0], mapRatingToRange(x[1][0] / x[1][1]), x[1][0] / x[1][1]))

rdd.saveAsTextFile(sys.argv[2])
