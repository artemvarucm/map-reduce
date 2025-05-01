from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date, hour, lower, split, count, row_number
import sys
from pyspark.sql.window import Window
# datos sacados de aqui: https://www.gharchive.org

# cogido de aquí: https://gist.github.com/sebleier/554280
stop_words_english = [
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", \
    "you", "your", "yours", "yourself", "yourselves", "he", "him", \
    "his", "himself", "she", "her", "hers", "herself", "it", "its", \
    "itself", "they", "them", "their", "theirs", "themselves", "what", \
    "which", "who", "whom", "this", "that", "these", "those", "am", "is", \
    "are", "was", "were", "be", "been", "being", "have", "has", "had", \
    "having", "do", "does", "did", "doing", "a", "an", "the", "and", \
    "but", "if", "or", "because", "as", "until", "while", "of", "at", \
    "by", "for", "with", "about", "against", "between", "into", "through", \
    "during", "before", "after", "above", "below", "to", "from", "up", "down", \
    "in", "out", "on", "off", "over", "under", "again", "further", "then", \
    "once", "here", "there", "when", "where", "why", "how", "all", "any", \
    "both", "each", "few", "more", "most", "other", "some", "such", "no", \
    "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t",\
    "can", "will", "just", "don", "should", "now"\
    # ESPECIFICO PARA GITHUB 
    "github", "com",
    "fix", "update", "add", "remove", "refactor", "change", "improve", "make", "use",
    "clean", "bump", "merge", "pull", "push", "test", "build", "release", "upgrade",
    "initial", "revert", "work", "move", "convert", "rename",
    "file", "files", "code", "commit", "version", "branch", "repo", "repository",
    "module", "script", "line", "tag", "class", "method", "function", "project",
    "minor", "major", "new", "old", "default", "final", "debug", "temporary", "latest",
    "next", "prev", "current", "base",
    "typo", "issue", "fixes", "closes", "adds", "removes", "docs", "doc", "testcase",
    "tests", "test", "ci", "workflow", "github", "action", "actions", "log", "logging",
    "dependancy", "updated", "readme", "dependabot", "bot", "issues", "dev"
]

TOP_K = 50 # puede ser un cmd argument
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
df_commits = df_commits.withColumn("day", to_date("timestamp")) \
                       .withColumn("hour", hour("timestamp"))

df_commits = df_commits.select("day", "hour", "commit.message")

# extraemos palabras
df_words = df_commits.withColumn("word", explode(split(lower(col("message")), "\\W+")))

# quitamos las palabras vacias y stopwords
df_words = df_words.filter((col("word") != "") & (~col("word").isin(stop_words_english)))
df_words = df_words.filter(~col("word").rlike("^[0-9]+$"))
df_counts = df_words.groupBy(["day", "hour", "word"])\
    .agg(count("*").alias("count"))

window_spec = Window.partitionBy("day", "hour").orderBy(col("count").desc())

# seleccionamos por cada hora y dia las TOP_K palabras más repetidas
df_top_words = df_counts.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") <= TOP_K) \
    .drop("rank")
df_top_words.write.csv(sys.argv[2], header=True, mode="overwrite")