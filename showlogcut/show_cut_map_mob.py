import sys
#import MySQLdb
import time
import threading
import re
import gc
import copy
import multiprocessing

tp1 = ('ip','user_id','imei','mac','idfv')
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
    record = {}
    f = open(path,'r')
    strs = list()
    dic ={}
    for unit in f.readlines():
        if len(unit) > 1:
            strs.extend(unit.split("\""))
    count = len(strs)
    for index in range(count):
        if "\n" not in strs[index]:
            if 'scribe_meta<new_logfile>' in strs[index]:
                break;
            if 'tid' == strs[index]:
                dic['tid'] = strs[index+2]                                
            if 'channel_sources' == strs[index]:
                if len(strs[index+1]) == 1:dic['channel_source'] = strs[index+2]
                else:dic['channel_source'] = strs[index+1][1:-1]
            if 'ip' == strs[index]:
                dic['ip'] = strs[index+1][1:-1]
            if 'user_id' == strs[index]:
                if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
                else:dic['user_id'] = strs[index+1][1:-1]
            if 'user_agent' == strs[index]:
                if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
                else:dic['user_agent'] = strs[index+1][1:-1] 
            if 'imei' == strs[index]:
                if len(strs[index+1]) == 1:dic['imei'] = strs[index+2] 
                else:dic['imei'] = strs[index+1][1:-1]
	    if 'mac_addr' == strs[index]:
		if len(strs[index+1]) == 1:dic['mac'] = strs[index+2] 
		else:dic['mac'] = strs[index+1][1:-1]  
	    if 'idfv' == strs[index]:
		if len(strs[index+1]) == 1:dic['idfv'] = strs[index+2] 
		else:dic['idfv'] = strs[index+1][1:5]	    
            if 'time' == strs[index]:
                dic['time'] = strs[index+1][1:-1]                
        else:
            if not dic.get('tid',-1) == -1:
                shows.append(dic)
            dic = {}
    
    f.close() 		
    summry(shows,record,lock)
    
def summry(shows,record,lock):
    global tp1
    for v2 in shows:
        tidList = v2['tid'].split(',')
        try:
            dateConvert = time.localtime(int(v2['time']))
        except:
            return
        for tid in tidList:
	    record.setdefault(tid,{})
	    if int(v2['channel_source'])>>30 == 2 and not re.match(r'.*(php|java|javascript|perl|python|bingbot|Soso|Yahoo!|Sogou*.spider|Googlebot|Baiduspider|Baiduspider|EasouSpider).*',v2['user_agent'],re.M|re.I):	
		for tp in tp1:
		    record[tid].setdefault(tp+"count",0)
		    record[tid].setdefault(tp+"collection",{})
		record[tid].setdefault('count',0)
		record[tid].setdefault('totalcount',0)
		record[tid]['totalcount'] += 1
		record[tid].setdefault('pccount',0)
		record[tid].setdefault('ilcount',0)
		record[tid].setdefault('timecollection',{})	
		record[tid].setdefault('illegalcount',0)
		for tp in tp1:
		    record[tid][tp+"collection"].setdefault(v2[tp],0)
		try:
		    record[tid]['timecollection'].setdefault(dateConvert[3],0)    
		    record[tid]['count'] += 1
		    record[tid]['timecollection'][dateConvert[3]] += 1
		except ValueError:
		    pass
		try:
		    if not int(v2['ip']) == 2130706433:
			record[tid]['ipcount'] += 1
		    record[tid]['ipcollection'][v2['ip']] += 1
		except:
		    pass 
		if not str(v2['user_id']) == '0':
		    record[tid]['user_idcount'] += 1
		    record[tid]['user_idcollection'][v2['user_id']] += 1
		if not (str(v2['mac']) == '02:00:00:00:00:00' or str(v2['mac']) == '00:00:00:00:00:00'):
		    record[tid]['maccount'] += 1
		    record[tid]['maccollection'][v2['mac']] += 1
		if not (str(v2['imei']) == 'null'):
		    record[tid]['imeicount'] += 1
		    record[tid]['imeicollection'][v2['imei']] += 1
		if not str(v2['idfv']) == 'null':
		    record[tid]['idfvcount'] += 1
		    record[tid]['idfvcollection'][v2['idfv']] += 1		    
	    elif not re.match(r'.*(php|java|javascript|perl|python|bingbot|Soso|Yahoo!|Sogou*.spider|Googlebot|Baiduspider|Baiduspider|EasouSpider).*',v2['user_agent'],re.M|re.I):
		record[tid].setdefault('count',0)
		record[tid].setdefault('totalcount',0)
		record[tid]['totalcount'] += 1		
    middle = {}
    
    for tid,item in record.items():
        if not any(item) or item['count'] == 0:
	    continue
        try:
            num = int(tid)%1013
        except:
            num = -1
        str_write = "tid:"+str(tid)+","
        str_write += "count:"+str(item['count'])+","
        str_write += 'timecollection:'
        for hour,time_collection in item['timecollection'].items():
            str_write += str(hour)+'-'+str(time_collection)+"|"
        str_write += ","
        str_write += 'ipcount:'+str(item['ipcount'])+","
        str_write += 'ipcollection:'
        for hour,time_collection in item['ipcollection'].items():
            str_write += str(hour)+'-'+str(time_collection)+"|"
        str_write += ','
        str_write += 'user_idcount:'+str(item['user_idcount'])+","
        str_write += 'user_idcollection:'
        for hour,time_collection in item['user_idcollection'].items():
            str_write += str(hour)+'-'+str(time_collection)+"|" 
	str_write += ',totalcount:'+str(item['totalcount'])+","
	str_write += 'imeicount:'+str(item['imeicount'])+","
	str_write += 'imeicollection:'
	for hour,time_collection in item['imeicollection'].items():
	    str_write += str(hour)+'-'+str(time_collection)+"|"	
	str_write += ','
	str_write += 'maccount:'+str(item['maccount'])+","
	str_write += 'maccollection:'
	for hour,time_collection in item['maccollection'].items():
	    str_write += str(hour)+'-'+str(time_collection)+"|"	
	str_write += ','
	str_write += 'idfvcount:'+str(item['idfvcount'])+","
	str_write += 'idfvcollection:'
	for hour,time_collection in item['idfvcollection'].items():
	    str_write += str(hour)+'-'+str(time_collection)+"|"		
        middle.setdefault(num,[])
        middle[num].append(str_write)
    lock.acquire()
    for key,value in middle.items():
        try:
            if int(key) < 10:
                f1 = open('/home/brdwork/antispider/data/showsmap/range_mob-000'+str(key),'a+')
            elif int(key) < 100:
                f1 = open('/home/brdwork/antispider/data/showsmap/range_mob-00'+str(key),'a+')
            elif int(key) <1000:
                f1 = open('/home/brdwork/antispider/data/showsmap/range_mob-0'+str(key),'a+')
            else:
                f1 = open('/home/brdwork/antispider/data/showsmap/range_mob-'+str(key),'a+')
            for value1 in value:
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
