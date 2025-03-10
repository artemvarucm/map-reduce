import sys

def mapRatingToRange(rating):
    if rating <= 1:
        return "Range-1"
    elif rating <= 2:
        return "Range-2"
    elif rating <= 3:
        return "Range-3"
    elif rating <= 4:
        return "Range-4"
    else:
        return "Range-5"

for line in sys.stdin:
    line = line.replace('\n', '')
    key, avg, cnt = line.split('\t')
    print(key + "\t" + mapRatingToRange(float(avg)) + "\t" + avg)
