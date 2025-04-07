from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, ceil
import sys


spark = SparkSession.builder \
    .appName("Ej4") \
    .getOrCreate()

sc = spark.sparkContext
df = spark.read.csv(sys.argv[1], header=True, sep=",")
# asignar range despues de la media
df = df.withColumn("rating-label", concat(lit("Range-" ), ceil(col("rating"))))
df = df.replace("Range-0", "Range-1")
df.show(5)

df.groupBy("rating-label")\
    .agg({"rating": "avg"})\
    .withColumnRenamed("avg(rating)", "Average")\
    .select("rating-label", "Average")\
    .write.csv(sys.argv[2], header=True, mode="overwrite")
