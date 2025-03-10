#!/usr/bin/python

import sys
import re

for line in sys.stdin:
    words = re.sub(r'\W+', ' ', line).split()
    
    for word in words:
        print(word.lower() + "\t1")
