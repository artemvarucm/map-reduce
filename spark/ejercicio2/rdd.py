from pyspark.sql import SparkSession
import sys
import re
TOP_K = 20

def extract_url(line):
    urlLine = re.search(r'"(.*)"', line)
    if urlLine:
        url = urlLine.group(1).split(" ")[1] # GET /index?search=1 HTTP -> se queda con /index?search=1
        urlNoQuery = url.split("?")[0] # elimina el query -> se queda con /index
        return urlNoQuery
    return None

spark = SparkSession.builder \
    .appName("Ej2") \
    .getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(sys.argv[1])

urls_rdd = rdd.map(extract_url).filter(lambda x: x is not None)
url_counts = urls_rdd.map(lambda url: (url, 1)).reduceByKey(lambda a, b: a + b)
top_urls = url_counts.takeOrdered(TOP_K, key=lambda x: -x[1])
with open(sys.argv[2], "w") as file:
    for url in top_urls:
        file.write(str(url))
        file.write('\n')