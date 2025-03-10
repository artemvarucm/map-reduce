import sys
# cat ml-latest-small/ratings.csv | python3 mapperJob1.py | sort | python3 reducerJob1.py | python3 mapperJob2.py
for line in sys.stdin:
    line = line.replace('\n', '')
    csvLine = line.split(",")
    if (csvLine[0] != "userId"):
        movieId = csvLine[1]
        rating = csvLine[2]
        print(movieId + "\t" + rating + "\t1")