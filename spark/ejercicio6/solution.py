from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date, lower, split, count
import sys
# https://www.gharchive.org
TOP_K = 100

spark = SparkSession.builder.appName("Ej6").getOrCreate()
sc = spark.sparkContext
df = spark.read.json(sys.argv[1])

# cogemos eventos que son push, porque son los que tienen commits
df_commits = df.filter(col("type") == "PushEvent")
# pueden haber varios commits
df_commits = df_commits.select(
    to_timestamp(col("created_at")).alias("timestamp"),
    explode("payload.commits").alias("commit")
)
df_commits = df_commits.withColumn("day", to_date("timestamp"))
df_commits = df_commits.select("day", "commit.message")

# extraemos palabras
df_words = df_commits.withColumn("word", explode(split(lower(col("message")), "\\W+")))

# quitamos las palabras vacias
df_words = df_words.filter(col("word") == "")

# seleccionamos por cada dia las TOP_K palabras más repetidas
df_words.groupBy(["day", "word"])\
    .agg(count("*").alias("count"))\
    .orderBy(col("count").desc())\
    .limit(TOP_K)\
    .write.csv(sys.argv[2], header=True, mode="overwrite")