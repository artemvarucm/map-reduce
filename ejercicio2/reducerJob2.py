#!/usr/bin/python
# Mismo codigo que mapper. Utilizar 1 reducer (-numReduceTasks 1) al ejecutar.
import sys
import bisect
TOP_K = 20

list = []
for line in sys.stdin:
    line = line.replace('\n', '')
    url, cnt = line.split('\t')
    cnt = int(cnt)
    # insert record in list sorted by f
    bisect.insort(list, {"url": url, "cnt": cnt}, key=lambda x: x["cnt"])
    # truncate list to top K
    if len(list) > TOP_K:
        list = list[-TOP_K:]

for value in list:
    print(value["url"] + "\t" + str(value["cnt"]))