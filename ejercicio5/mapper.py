#!/usr/bin/python

import sys
# cat Meteorite_Landings.csv | python3 mapper.py | sort | python3 reducer.py
for line in sys.stdin:
    line = line.replace('\n', '')
    csvLine = line.split(";")
    if (csvLine[0] != "name"):
        met_type = csvLine[3] # usamos recclass como tipo (preguntar)
        mass = csvLine[4]

        # se filtra aquí también (preguntar patrón)
        if (mass != ""):
            print(met_type + "\t" + mass + "\t1")


         