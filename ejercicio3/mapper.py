#!/usr/bin/python

import sys
# cat GOOG.csv | python3 mapper.py | python3 reducer.py
for line in sys.stdin:
    line = line.replace('\n', '')
    csvLine = line.split(",")
    if (csvLine[0] != "Date"):
        year = csvLine[0][:4]
        close = csvLine[4]
        print(year + "\t" + close + "\t1")