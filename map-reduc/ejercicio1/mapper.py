#!/usr/bin/python

# cat test.txt | python3 mapper.py ASD
# cat test.txt | python3 mapper.py Asd

import sys, re

if len(sys.argv) == 2:
    palabra = sys.argv[1].lower()
    for line in sys.stdin:
        line = line.replace('\n', '')
        words = re.sub(r'\W+', ' ', line.lower()).split()
        if palabra in words:
            print(line)