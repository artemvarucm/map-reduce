#!/usr/bin/python

import sys

prev = ""
count = 0

for line in sys.stdin:
    key, value = line.split('\t')
    
    if key == prev:
        count += int(value)
    else:
        if prev != "":
            print(prev + '\t' + str(count))
        prev = key
        count = int(value)

print(prev + '\t' + str(count))
