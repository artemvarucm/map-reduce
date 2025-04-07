from pyspark import SparkConf, SparkContext
import re, sys

conf = SparkConf().setAppName('WordCount')
sc = SparkContext.getOrCreate(conf)

search_str = sys.argv[1].lower()
rdd = sc.textFile(sys.argv[2])
rdd_partition = rdd.coalesce(4)
words = rdd_partition.filter(lambda line: search_str in re.sub(r'\W+', ' ', line.lower()).split())
words.saveAsTextFile(sys.argv[3])

