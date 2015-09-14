import sys 
import os
f = open('2015-09-07.csv','r')
x = f.readlines()
f.close() 
f = open('2015-09-07.csv','w+')
for y in x:
    try:
	f.write(y.decode('utf-8').encode('gb2312'))
    except:
	f.write(y)
f.close()
