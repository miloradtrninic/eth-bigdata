#!/usr/bin/python

import sys

last_key = None
value_sum = 0
this_key = None

for input_line in sys.stdin:
    try:
        value, this_key = input_line.split('\t', 1)
        value = int(value)
    except ValueError:
        continue
    print("%s\t%d" % (this_key, value))

    if last_key == this_key:
        value_sum = value
    else:
        if last_key:
            print("%s\t%d" % (last_key, value_sum))
        value_sum = value
        last_key = this_key