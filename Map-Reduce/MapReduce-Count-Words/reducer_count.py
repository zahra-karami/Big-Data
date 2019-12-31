#!/usr/bin/env python
"""reducer_count.py"""

import collections
import sys

counter = collections.Counter()

for line in sys.stdin:
    line = line.strip()
    count,word = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:
        continue

    counter[word] += count

common = counter.most_common(10)
for item in common:
    print("%s\t%s" % (item[0] , item[1]))


    