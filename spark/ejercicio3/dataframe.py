from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("Ej3") \
    .getOrCreate()

sc = spark.sparkContext
df = spark.read.csv(sys.argv[1], header=True)

df = df.withColumn("Year", df["Date"].substr(1, 4))\
    .withColumn("Close", df["Close"].cast("float"))

df.groupBy("Year")\
    .agg({"Close": "avg"})\
    .withColumnRenamed("avg(Close)", "Average")\
    .select("Year", "Average")\
    .write.csv(sys.argv[2], header=True, mode="overwrite")
