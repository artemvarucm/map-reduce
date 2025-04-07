from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Ej5") \
    .getOrCreate()

sc = spark.sparkContext
df = spark.read.csv(sys.argv[1], header=True, sep=";")

df = df.withColumn("mass", df["mass (g)"].cast("float"))
df = df.dropna(subset=["mass"])
df.groupBy("recclass")\
    .agg({"mass": "avg"})\
    .withColumnRenamed("avg(mass)", "Average")\
    .select("recclass", "Average")\
    .write.csv(sys.argv[2], header=True, mode="overwrite")
