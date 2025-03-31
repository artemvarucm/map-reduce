#!/usr/bin/python

import sys
# cat Meteorite_Landings.csv | python3 mapper.py | sort | python3 reducer.py
for line in sys.stdin:
    line = line.replace('\n', '')
    csvLine = line.split(";")
    if (csvLine[0] != "name"):
        met_type = csvLine[3] 
        mass = csvLine[4]
        if (mass != ""):
            print(met_type + "\t" + mass + "\t1")