from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, ceil
import sys


spark = SparkSession.builder \
    .appName("Ej4") \
    .getOrCreate()

sc = spark.sparkContext
df = spark.read.csv(sys.argv[1], header=True, sep=",")
df = df.groupBy("movieId").agg({"rating": "avg"}).withColumnRenamed("avg(rating)", "Average")
df = df.withColumn("rating-label", concat(lit("Range-" ), ceil(col("Average"))))
df = df.replace("Range-0", "Range-1") # el valor 0.0 pertenece al rango 1
df.select("movieId", "rating-label", "Average")\
    .write.csv(sys.argv[2], header=True, mode="overwrite")
