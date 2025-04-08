from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import regexp_extract, split, count, col
import sys
TOP_K = 20

spark = SparkSession.builder \
    .appName("Ej2") \
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(sys.argv[1])
df = spark.createDataFrame(rdd, StringType())

df = df.withColumn("quoted", regexp_extract(col("value"), r'"(.*)"', 1))
df = df.withColumn("url", split(col("quoted"), " ").getItem(1))
df = df.withColumn("url_no_query", split(col("url"), r"\?").getItem(0))
# igual hay que añadir algun filtro...
top_urls = df.groupBy("url_no_query")\
      .agg(count("*").alias("access_count"))\
      .orderBy(col("access_count").desc())\
      .limit(TOP_K)


top_urls.write.csv(sys.argv[2], header=True, mode="overwrite")