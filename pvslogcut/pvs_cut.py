import sys
import MySQLdb
import time
import threading
import re
import gc


tp1 = ('imei','idfv','mac_addr','ip','user_id')
record = {}
date = time.time()-15*86400
#date = time.time()
dateList = time.localtime(date)
if int(dateList[1]) < 10:
    if int(dateList[2]) < 10:
   	dateStr = str(dateList[0])+"-0"+str(dateList[1])+"-0"+str(dateList[2])+"_00"
    else:
   	dateStr = str(dateList[0])+"-0"+str(dateList[1])+"-"+str(dateList[2])+"_00"
	
else:
    if int(dateList[2]) < 10:
   	dateStr = str(dateList[0])+"-"+str(dateList[1])+"-0"+str(dateList[2])+"_00"
    else:
   	dateStr = str(dateList[0])+"-"+str(dateList[1])+"-"+str(dateList[2])+"_00"
path = "/data/scribelog/brd_ad/cpc/brdad_cpc_all_tid_shop_pvs/brdad_cpc_all_tid_shop_pvs-"+dateStr
#path = "../../logs/brdad_cpc_all_tid_shop_pvs-"+dateStr	

def splitWithLog(path):
    shows = list()
    f = open(path,'r')
    strs = list()
    dic ={}
    for unit in f.readlines():
	if unit.find('@') == -1 and len(unit) > 1:
	    strs.extend(unit.split("\""))
    count = len(strs)
    for index in range(count):
	if "\n" not in strs[index]:
	    if 'scribe_meta<new_logfile>' in strs[index]:
		break;
	    if 'tid' in strs[index]:
		dic['tid'] = strs[index+2]                                
	    if 'channel_source' in strs[index]:
		if len(strs[index+1]) == 1:dic['channel_source'] = strs[index+2]
		else:dic['channel_source'] = strs[index+1][1:-1]
	    if 'ip' in strs[index]:
		dic['ip'] = strs[index+1][1:-1]
	    if 'mac_addr' in strs[index]:
		if len(strs[index+1]) == 1:dic['mac_addr'] = strs[index+2]
		else:
		    dic['mac_addr'] = strs[index+1][1:-1]
	    if ('imei' in strs[index]) and len(strs[index]) < 10:
		if len(strs[index+1]) == 1:dic['imei'] = strs[index+2]
		else:dic['imei'] = strs[index+1][1:-1]
	    if 'idfv' in strs[index]:
		if len(strs[index+1]) == 1:dic['idfv'] = strs[index+2]
		else:dic['idfv'] = strs[index+1][1:5]
	    if 'user_id' in strs[index]:
		if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
		else:dic['user_id'] = strs[index+1][1:-1]
	    if 'user_agent' in strs[index]:
		if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
		else:dic['user_agent'] = strs[index+1][1:-1] 
	    if 'time' in strs[index]:
		dic['time'] = strs[index+1][1:-1]                
	else:
	    if not dic.get('tid',-1) == -1:
		shows.append(dic)
	    dic = {}
    """scanner.dump_all_objects('dump' )
    om = loader.load('dump',using_json=None,show_prog=False,collapse=True)
    summarize = om.summarize() 
    lista = om.get_all('str');
    print(summarize)
    print("\n")
    for item in lista:
	print(item)
	print("\n")"""
    f.close() 
    del strs
    del dic		
    summry(shows)
    del shows
    gc.collect()

		
def summry(shows):
    global record
    global tp1
    for v2 in shows:
	try:
	    dateConvert = time.localtime(int(v2['time']))
	except:
	    return
	record.setdefault(v2['tid'],{})
	record[v2['tid']].setdefault('tid',v2['tid'])
	for tp in tp1:
	    record[v2['tid']].setdefault(tp+"count",0)
	    record[v2['tid']].setdefault(tp+"collection",{})
	    record[v2['tid']][tp+"collection"].setdefault(v2[tp],0)
	record[v2['tid']].setdefault('count',0)
	record[v2['tid']].setdefault('pccount',0)
	record[v2['tid']].setdefault('ilcount',0)
	record[v2['tid']].setdefault('timecollection',{})	
	record[v2['tid']].setdefault('illegalcount',0)
	for index in range(0,24):
	    record[v2['tid']]['timecollection'].setdefault(index,0)
	try:
	    #record[v2['tid']]['timecollection'].setdefault(dateConvert[3],0)
	
            record[v2['tid']]['count'] += 1
	    record[v2['tid']]['timecollection'][dateConvert[3]] += 1
	except ValueError:
	    pass
	try:
	    if not int(v2['ip']) == 2130706433:
		record[v2['tid']]['ipcount'] += 1
		record[v2['tid']]['ipcollection'][v2['ip']] += 1
	except:
	    pass
	try:
	    if re.match(r'.*(php|java|javascript|perl|python|bingbot|Soso|Yahoo!|Sogou*.spider|Googlebot|Baiduspider|Baiduspider|EasouSpider).*',v2['user_agent'],re.M|re.I):
		record[v2['tid']]['illegalcount'] += 1
	    if re.match(r'.*[g-z].*',v2['mac_addr'],re.M|re.I):
		record[v2['tid']]['illegalcount'] += 1		
	except:
	    pass 
	if not str(v2['imei']) == 'null':
	    record[v2['tid']]['imeicount'] += 1
	    record[v2['tid']]['imeicollection'][v2['imei']] += 1
	if not str(v2['user_id']) == '0':
	    record[v2['tid']]['user_idcount'] += 1
	    record[v2['tid']]['user_idcollection'][v2['user_id']] += 1
	if not (str(v2['mac_addr']) == '02:00:00:00:00:00' or str(v2['mac_addr']) == 'null' or str(v2['mac_addr'])=='00:00:00:00:00:00'):
	    record[v2['tid']]['mac_addrcount'] += 1
	    record[v2['tid']]['mac_addrcollection'][v2['mac_addr']] += 1
	if not str(v2['idfv']) == 'null':
	    record[v2['tid']]['idfvcount'] += 1
	    record[v2['tid']]['idfvcollection'][v2['idfv']] += 1 
	if int(v2['channel_source'])>>30 == 1:
	    record[v2['tid']]['pccount'] += 1
    gc.collect()
    
def caculate():
    
    db = MySQLdb.connect("172.16.0.188","mlsreader","RMlSxs&^c6OpIAQ1","test",3316 )
    cursor = db.cursor()
    global record
    time_avrege = {}
    global tp1
    countSuccess = 0
    countFailed = 0
    excuteRecord = []
    for key,item in record.items():
	outputItem = {}
	timeMax = 0
	outputItem.setdefault('tid',item['tid'])
	outputItem.setdefault('pvs',item['count'])
	outputItem.setdefault('date',str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
	try:
	    outputItem.setdefault('illegal',item['illeagalcount'])
	except:
	    outputItem.setdefault('illegal',0)
	if str(key)  == '3668029553':
	    ipcollection = {}
	    for ip in sorted(item['ipcollection'].keys()):
		if ip == '':
		    ip = 0
		for key,mark in ipcollection.items():
		    if abs(int(key) - int(ip)) < 10:
			ipcollection[ip] = mark
			break;
		else:
		    ipcollection[ip] = ip
	    ipcollection1 = {}
	    for key,mark in ipcollection.items():
		if mark in ipcollection1.keys():
		    ipcollection1[mark].append(key)
		else:
		    ipcollection1.setdefault(mark,[])
		    ipcollection1[mark].append(key)
	    for x,y in ipcollection1.items():
		print str(x)+"   "+str(y)
	    exit()
	try:
	    outputItem.setdefault('pc_rate',round(float(item['pccount'])/float(item['count']),2)*100)
	    outputItem.setdefault('pc_count',item['pccount'])
	except:
	    outputItem.setdefault('pc_rate',-1)
	    outputItem.setdefault('pc_count',-1)
	for tp in tp1:
	    maxK = ''
	    maxV = 0
	    for k1,v1 in item[tp+'collection'].items():
		if(v1 > maxV):
		    
		    maxV = v1
		    maxK = k1
			
	    if not item[tp+'count'] == 0:
		outputItem.setdefault(tp+'_total_count',item[tp+'count'])
		outputItem.setdefault(tp+'_count',0)
		for it in item[tp+'collection'].values():
		    if it != 0:
			outputItem[tp+'_count']+=1
		outputItem.setdefault(tp+'_max',maxK+"|"+str(float(maxV)))
	    else:
		outputItem.setdefault(tp+'_total_count',item[tp+'count'])
		outputItem.setdefault(tp+'_count',0)
		outputItem.setdefault(tp+'_max',0)
	time_avrege.setdefault('count',{})
	time_avrege.setdefault('tongji',{})
	for index in range(24):
	    time_avrege['tongji'].setdefault(index,0)
	    time_avrege['count'].setdefault(index,0)
	    if not index == 23:
	    
		try:
		    time_avrege['count'][index] += 1
		    time_avrege['tongji'][index] +=float(item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))/float(outputItem['pvs'])
		    if((item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))>timeMax):
		        timeMax = item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0)
	    	except:
		    pass
	    elif (index == 23):
                try:
		    time_avrege['count'][index] += 1
                    time_avrege['tongji'][index]+= float(item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))/float(outputItem['pvs'])
                    if((item['timecollection'].get(index,0)+item['timecollection'].get(0,0))>timeMax):
                        timeMax = item['timecollection'].get(index,0)+item['timecollection'].get(0,0)
                except:
                    pass	
	try:
	    time_avrege['tongji'].setdefault(24,0)
	    time_avrege['tongji'][24] += float(timeMax)/float(outputItem['pvs'])
	    outputItem.setdefault('timeGroup',float(timeMax)/float(outputItem['pvs']))
	except:
	    outputItem.setdefault('timeGroup',-1)
    for index in range(24):
	print str(time_avrege['tongji'][index])+"     "+str(time_avrege['count'][index]) 
    excuteRecord.append(countSuccess)
    print str(time_avrege['tongji'][24])
    excuteRecord.append(countFailed)
    return excuteRecord
def splitLog(path):  
    excuteRecord = list() 
    for index in range(200):
	if len(str(index)) == 1:
	    num = "00"+str(index)
	else:
	    if len(str(index)) == 2:
		num = "0"+str(index)
	    else:num = str(index)
	try:    
	    splitWithLog(path+num)
	    excuteRecord.append(dateStr+str(index)+"completed\n")
	    print(dateStr+str(index)+"completed")
	except IOError:
	    continue
    gc.collect()
    return excuteRecord
	
threadExeList = splitLog(path)
itemSuccessList = caculate()
f = open("/home/brdwork/logcut/pvslog/opreation",'a+')
f.write('opreation_time_'+str(dateList[0])+'_'+str(dateList[1])+'_'+str(dateList[2])+'_begin:'+"\n")
for item in threadExeList:
    f.write(str(item)+"\n")
f.write('Success:'+str(itemSuccessList[0])+"\n")
f.write('Failed:'+str(itemSuccessList[1])+"\n")
f.write("END"+"\n")
f.close()
