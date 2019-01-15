#!/usr/bin/python

import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) == 11:
            #to_address #value
            print("%s\t%s" % (fields[6], fields[7]))
