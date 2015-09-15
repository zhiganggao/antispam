import sys
#import MySQLdb
import time
import string
import math
import re


f = open('oui.txt','r')
e = open('mac_list','w+')
g = []
for line in f.readlines():
    if 'base 16' in line:
        g.append(string.lstrip(line)[0:6])
g = g[:-1]
print '\n'.join(g)
for writeline in g:
    e.write(writeline + "\n")
e.flush()
e.close()
f.close()