from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Ej6").getOrCreate()


df = spark.read.csv("./out/part-00003-e85fcdc2-175a-44cb-9fce-a08eaedf97d5-c000.csv", header=True, multiLine = True) 
print(df.select("@ message @").show(3, truncate=False))