"""
Procesa mensajes de commits en eventos PushEvent de GitHub a partir de un conjunto de datos en formato JSON.
Genera una codificación tipo bolsa de palabras (una fila por mensaje de commit) utilizando las N palabras más frecuentes.
Se puede controlar el tamaño del vocabulario (N) y excluir las palabras vacías (stopwords).

Uso:
    spark-submit solution.py <ruta_entrada> <ruta_salida> <tam_vocabulario> <ignore_stopwords>
    
Argumentos:
    ruta_entrada        Ruta a los archivos JSON de entrada desde GHArchive
    ruta_salida         Ruta donde se guardarán los archivos CSV de salida
    tam_vocabulario     Tamaño del vocabulario, cuántas columnas de la bolsa de palabras habrá
    ignore_stopwords    1 o 0 (indica si se deben ignorar las stopwords)

IMPORTANTE: CUANDO SE LEA EL ARCHIVO DE SALIDA USAR multiLine = True en spark.read.csv

"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split, count, desc, concat_ws, broadcast
from time import time
# Argumentos
input_path = sys.argv[1]
output_path = sys.argv[2]
TOP_K = int(sys.argv[3])
REMOVE_STOPWORDS = sys.argv[4].lower() == "1"


stop_words_english = [ # https://gist.github.com/sebleier/554280
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
    "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
    "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom",
    "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but",
    "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about",
    "against", "between", "into", "through", "during", "before", "after", "above", "below", "to",
    "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few",
    "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
    "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now",
    
    # para GitHub
    "github", "com", "fix", "update", "add", "remove", "refactor", "change", "improve", "make", "use",
    "clean", "bump", "merge", "pull", "push", "test", "build", "release", "upgrade", "initial",
    "revert", "work", "move", "convert", "rename", "file", "files", "code", "commit", "version",
    "branch", "repo", "repository", "module", "script", "line", "tag", "class", "method", "function",
    "project", "minor", "major", "new", "old", "default", "final", "debug", "temporary", "latest",
    "next", "prev", "current", "base", "typo", "issue", "fixes", "closes", "adds", "removes", "docs",
    "doc", "testcase", "tests", "ci", "workflow", "action", "actions", "log", "logging", "dependancy",
    "updated", "readme", "dependabot", "bot", "issues", "dev"
]

spark = SparkSession.builder.appName("Ej6").getOrCreate()

# datos sacados de aqui: https://www.gharchive.org
df = spark.read.json(input_path) 
t_0 = time()
# Solo cogemos eventos de commits y convertimos la lista de commits en muchas filas con explode:
df_commits = df.filter(col("type") == "PushEvent") \
    .select(explode("payload.commits").alias("commit"))

# porque si no pueden haber una palabra asi
idColName = "@ id @" 
messageColName = "@ message @"

# id = sha + message 
# identificador por cada mensaje para que si aparecen 2 mensajes iguales, contribuyan igual a la frecuencia
df_messages = df_commits.select(col("commit.message").alias(messageColName), concat_ws("-", col("commit.sha"), col("commit.message")).alias(idColName)).dropna()

df_words_encoded = df_messages.withColumn("word", explode(split(lower(col(messageColName)), "\\W+"))).filter(col("word") != "")
df_words_encoded = df_words_encoded.filter(~col("word").rlike("^[0-9]+$")) # excluimos palabras que son numeros (ej. 2025)

if REMOVE_STOPWORDS: # quitamos stop words
    df_words_encoded = df_words_encoded.filter(~col("word").isin(stop_words_english))

# vocabulario (top_k palabras mas frecuentes)
vocabulary = df_words_encoded.groupBy("word") \
    .agg(count("*").alias("total_count")) \
    .orderBy(desc("total_count")) \
    .limit(TOP_K)

# para acelerar el pivot
vocabulary_list = [row["word"] for row in vocabulary.collect()]

# solo nos quedamos con palabras que entran en el vocabulario
df_top_word_counts = df_words_encoded.filter(col("word").isin(vocabulary_list)) \
    .groupBy(idColName, "word").agg(count("*").alias("count"))

# creamos la bolsa de palabras
df_top_encoding = df_top_word_counts.groupBy(idColName).pivot("word", vocabulary_list).sum("count").na.fill(0)

# añadimos la bolsa de palabras a cada mensaje
df_encoded_with_text = df_messages.join(df_top_encoding, on=idColName).drop(idColName)
df_encoded_with_text.write.csv(output_path, header=True, mode="overwrite")
print(time() - t_0, "seconds elapsed")
