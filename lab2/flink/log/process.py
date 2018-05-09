#!/usr/bin/python
  
import sys
import time

file = open(sys.argv[1])
while 1:
    line = file.readline().rstrip()
    if not line: break
    time.sleep(0.1)
    print line
file.close()
