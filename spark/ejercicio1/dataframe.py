from pyspark import SparkConf, SparkContext
import re, sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()
sc = spark.sparkContext
search_str = sys.argv[1].lower()
rdd = sc.textFile(sys.argv[2])
# reduces the number of partitions without shuffling data across the cluster. 
rdd_partition = rdd.coalesce(4)
df = spark.createDataFrame(rdd_partition, StringType())
words = df.filter(lambda line: search_str in re.sub(r'\W+', ' ', line.lower()).split())

# Save the results to the output file
words.saveAsTextFile(sys.argv[3])

