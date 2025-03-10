#!/usr/bin/python

import sys
import re
# cat access_log-small | python3 mapperJob1.py | sort | python3 reducerJob1.py | python3 mapperJob2.py | python3 reducerJob2.py

for line in sys.stdin:
    line = line.replace('\n', '')
    urlLine = re.search(r'"(.*)"', line)
    if urlLine:
        url = urlLine.group(1).split(" ")[1]
        urlNoQuery = url.split("?")[0] # elimina el query
        print(urlNoQuery.lower() + "\t1")