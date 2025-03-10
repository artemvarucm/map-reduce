#!/usr/bin/python

import sys
# combiner tiene el código igual al reducer

prev = ""
sum = 0
count = 0
for line in sys.stdin:
    line = line.replace('\n', '')
    key, avg_row, cnt_row = line.split('\t')

    if key == prev:
        sum += float(avg_row) * int(cnt_row)
        count += int(cnt_row)
    else:
        if prev != "":
            print(prev + '\t' + str(sum / count) + '\t' + str(count))
        prev = key
        sum = float(avg_row) * int(cnt_row)
        count = int(cnt_row)

print(prev + '\t' + str(sum / count) + '\t' + str(count))