from pyspark import SparkConf, SparkContext
import re, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.ml.feature import Tokenizer
from pyspark.sql.functions import col, lower

spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

sc = spark.sparkContext
search_str = sys.argv[1].lower()
rdd = sc.textFile(sys.argv[2])
rdd_partition = rdd.coalesce(4)
df = spark.createDataFrame(rdd_partition, StringType())

words = df.filter(lower(col("value")).rlike(rf"\b{search_str}\b"))
words.write.text(sys.argv[3])

