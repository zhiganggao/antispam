import sys
#import MySQLdb
import time
import threading
import re
import gc
import copy
import multiprocessing
import json
tp1 = ('ip','user_id')
date = time.time()-86400
dateList = time.localtime(date)
if int(dateList[1]) < 10:
    if int(dateList[2]) < 10:
        dateStr = str(dateList[0])+"-0"+str(dateList[1])+"-0"+str(dateList[2])+"_0"
    else:
        dateStr = str(dateList[0])+"-0"+str(dateList[1])+"-"+str(dateList[2])+"_0"

else:
    if int(dateList[2]) < 10:
        dateStr = str(dateList[0])+"-"+str(dateList[1])+"-0"+str(dateList[2])+"_0"
    else:
        dateStr = str(dateList[0])+"-"+str(dateList[1])+"-"+str(dateList[2])+"_0"
path = "/data/scribelog/brd_ad/cpc/brdad_cpc_all_tid_shop_shows/brdad_cpc_all_tid_shop_shows-"+dateStr
def splitWithLog(path,lock):
    shows = list()
    f = open(path,'r')
    strs = list()
    dic ={}
    for logItem in f.readlines():
	try:
	    tmp = logItem.split(']')[1]
	except:
	    continue
	try:
	    jsonCode = tmp.split('[')[0].lstrip()
	    dic = json.loads(jsonCode)
	except:
	    continue
	if not dic.get('tid','') == '':
	    try:
		shows.append(dic)
	    except:
		continue
	dic = {}
    f.close() 		
    summry(shows,lock)
    
def summry(shows,lock):
    allTotalCount = 0
    writeList = {} 
    record = {}
    for dic in shows:
	referTotalCount = 0
	tidList = dic['tid'].split(',')
	allTotalCount += len(tidList)
	for tid in tidList:
	    tid = str(tid) 
	    try:
		key = dic['tag_word']
		record.setdefault(key,{})
		#---------------------------ipcollection-------------
		record[key].setdefault('ip',{})
		record[key]['ip'].setdefault(dic['ip'],0)
		record[key]['ip'][dic['ip']] += 1
		#---------------------------platformcollection-------------
		record[key].setdefault('channel_sources',{})
		record[key]['channel_sources'].setdefault(dic['channel_sources'],0)
		record[key]['channel_sources'][dic['channel_sources']] += 1
		#---------------------------user_idcollection-------------
		record[key].setdefault('user_id',{})
		record[key]['user_id'].setdefault(dic['user_id'],0)
		record[key]['user_id'][dic['user_id']] += 1
	    except:
		info=sys.exc_info()  
	        print info[0],":",info[1]
		pass
    for key,item in record.items():
	num = 0 
	calculateNum = 0 
	markNum = 1 
	index = 0 
	try:
	    if len(key) == 0 or key == '' or key == None or key == 'null':
		key =str(item['key_id'])
	except:
	    continue
	for char in key:
	    if index  == 3:
		markNum = ord(char)
	    calculateNum += ord(char)
	    index += 1
	calculateNum *= len(key)* markNum
	num = calculateNum % 1013
	writeList.setdefault(num,[])
	writeList[num].append(json.dumps({key:item}))
    lock.acquire()
    for num,writeStr in writeList.items():
	try:
	    if int(num) < 10:
		f1 = open('/home/brdwork/antispider/data/showsmap/range-000'+str(num),'a+')
	    elif int(num) < 100:
		f1 = open('/home/brdwork/antispider/data/showsmap/range-00'+str(num),'a+')
	    elif int(num) <1000:
		f1 = open('/home/brdwork/antispider/data/showsmap/range-0'+str(num),'a+')
	    else:
		f1 = open('/home/brdwork/antispider/data/showsmap/range-'+str(num),'a+')
	    for value1 in writeStr:
		f1.write(value1+"\n")
	    f1.flush()
	    f1.close()
	except:
	    pass 
    lock.release()
    
def splitLog(path,range_x,lock):  
    for index in range(range_x[0],range_x[1]):            
        if len(str(index)) == 1:
            num = "000"+str(index)
        else:
            if len(str(index)) == 2:
                num = "00"+str(index)
            elif len(str(index)) == 3:
		num = "0"+str(index)
	    else:
		num = str(index)
        try: 

            splitWithLog(path+num,lock)         
            print(dateStr+str(index)+"completed")
        except IOError:
            continue
       
        gc.collect()

if __name__ == '__main__':
    lock = multiprocessing.Lock()
    record = []
    for i in range(5):
	process = multiprocessing.Process(target=splitLog,args=(path,[i*200,(i+1)*200],lock))
	process.start()
	record.append(process) 
    
    process = multiprocessing.Process(target=splitLog,args=(path,[1000,2000],lock))
    process.start()
    record.append(process)    
    for process in record:
	process.join()    
