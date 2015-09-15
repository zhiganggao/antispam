import sys
import MySQLdb
import time
import threading
import re
import gc

f = open("/home/brdwork/antispider/data/pvslog/opreationMOB",'a+')
if not "/home/brdwork/antispider/helper/" in sys.path:
    sys.path.append("/home/brdwork/antispider/helper/")
if not 'logCutHelper' in sys.modules:
    logCutHelper = __import__('logCutHelper')
else:
    eval('import logCutHelper')
    logCutHelper = eval('reload(logCutHelper)')
tp1 = ('ip','idfv','mac_addr','imei','user_id')
record = {}
date = time.time()-86400
dateList = time.localtime(date)
dateStr = str(dateList[0])+"-"+str(dateList[1])+"-"+str(dateList[2])
f.write('opreation time: '+time.asctime(time.localtime(time.time()))+"\n")
f2 = open('/home/brdwork/antispider/helper/mac_list','r')
mac_list = f2.readlines()
f2.close()
    
def caculate():
    global record
    dbwriter = logCutHelper.dbAccess()
    time_avrege = {}
    global tp1
    countSuccess = 0
    countFailed = 0
    excuteRecord = []
    for item in record.values():
	outputItem = {}
	timeMax = 0
	outputItem.setdefault('tid',item['tid'])
	outputItem.setdefault('pvs',item['count'])
	outputItem.setdefault('ilmac',item['ilmac'])
	outputItem.setdefault('date',str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
	try:
	    outputItem.setdefault('illegal',item['illeagalcount'])
	except:
	    outputItem.setdefault('illegal',0)
	ipcollection = {}
        for ip in sorted(item['ipcollection'].keys()):
            if ip == '': 
                ip = 0 
            for key,mark in ipcollection.items():
                if abs(int(key) - int(ip)) < 100: 
                    ipcollection[ip] = mark
                    break;
            else:
                ipcollection[ip] = ip
        distinct_ipcollection = {}
        for key,mark in ipcollection.items():
            distinct_ipcollection.setdefault(mark,0)
            if key == 0:
                key = ''
            distinct_ipcollection[mark] += int(item['ipcollection'][str(key)])
	maxK = ''
        maxV = 0
        for k1,v1 in distinct_ipcollection.items():
            if(v1 > maxV):

                maxV = v1
                maxK = k1
        if not item['ipcount'] == 0:
            outputItem.setdefault('ip_total_count',item['ipcount'])
            outputItem.setdefault('ip_count',0)
	    countindex = 0
            for it in distinct_ipcollection.values():
                if it != 0:
                    outputItem['ip_count']+=1
            outputItem.setdefault('ip_max',maxK+"|"+str(float(maxV)))
        else:
                outputItem.setdefault('ip_total_count',item['ipcount'])
                outputItem.setdefault('ip_count',0)
                outputItem.setdefault('ip_max',0) 
	for tp in tp1:
	    if tp == 'ip':
		continue
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
	time_avrege_temp = [0.094,0.046,0.029,0.019,0.016,0.019,0.029,0.044,0.05,0.068,0.089,0.095,0.107,0.112,0.109,0.117,0.118,0.101,0.091,0.102,0.127,0.144,0.14,0.131]
	for index in range(24):
	    time_avrege['tongji'].setdefault(index,0)
	    time_avrege['count'].setdefault(index,0)
	    if not index == 23:   
		try:
		    time_avrege['count'][index] += 1
		    time_avrege['tongji'][index] += float(item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))/float(outputItem['pvs'])
		    if((item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))>timeMax):
		        timeMax = item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0)
		    outputItem.setdefault('timeGroup',0)
		    outputItem['timeGroup'] += abs(float(item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))/float(outputItem['pvs']) - time_avrege_temp[index])
	    	except:
		    pass
	    elif (index == 23):
                try:
		    time_avrege['count'][index] += 1
                    time_avrege['tongji'][index]+= float(item['timecollection'].get(index,0)+item['timecollection'].get(0,0))/float(outputItem['pvs'])
                    if((item['timecollection'].get(index,0)+item['timecollection'].get(0,0))>timeMax):
                        timeMax = item['timecollection'].get(index,0)+item['timecollection'].get(0,0)
		    outputItem.setdefault('timeGroup',0)
		    outputItem['timeGroup'] += abs(float(item['timecollection'].get(index,0)+item['timecollection'].get(index+1,0))/float(outputItem['pvs']) - time_avrege_temp[index])		    
                except:
                    pass	
	try:
	    time_avrege['tongji'].setdefault(24,0)
	    time_avrege['tongji'][24] += float(timeMax)/float(outputItem['pvs'])
	    
	except:
	    outputItem.setdefault('timeGroup',-1)
	column = ('twitter_id','pvs','ip_total_count','ip_count','ip_max','user_total_count','user_count','user_max','mac_total_count','mac_count','mac_max','imei_total_count','imei_count','imei_max','idfv_total_count','idfv_count','idfv_max','time_deviation','createtime','illegal_count','ilmac')
	value = (int(outputItem['tid']),int(outputItem['pvs']),str(outputItem['ip_total_count']),str(outputItem['ip_count']),str(outputItem['ip_max']),str(outputItem['user_id_total_count']),str(outputItem['user_id_count']),str(outputItem['user_id_max']),str(outputItem['mac_addr_total_count']),str(outputItem['mac_addr_count']),str(outputItem['mac_addr_max']),str(outputItem['imei_total_count']),str(outputItem['imei_count']),str(outputItem['imei_max']),str(outputItem['idfv_total_count']),str(outputItem['idfv_count']),str(outputItem['idfv_max']),str(outputItem['timeGroup']),str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]),float(outputItem['illegal']),int(outputItem['ilmac']))
	if len(dbwriter.writeT('t_pvs_log_analysis_mob',column,value)) == 1:
	    countSuccess+=1
	else:	    
	    countFailed+=1
    excuteRecord.append(countSuccess)
    excuteRecord.append(countFailed)
    return excuteRecord
def splitLog(): 
    global record
    global f
    excuteRecord = list() 
    logcut = logCutHelper.logCut(2,1,date)
    for index in range(200):
	if len(str(index)) == 1:
	    num = "000"+str(index)
	else:
	    if len(str(index)) == 2:
		num = "00"+str(index)
	    else:num = '0'+str(index)
	try:    
	    record = logcut.summry((logcut.splitWithLog(num)),record)
	    excuteRecord.append(dateStr+str(index)+"completed\n")
	    print(dateStr+str(index)+"completed")
	except IOError:
	    continue
	except:
	    f.write('value error in summry\n')
    gc.collect()
    return excuteRecord
	
threadExeList = splitLog()
itemSuccessList = caculate()
f.write('opreation_time_'+str(dateList[0])+'_'+str(dateList[1])+'_'+str(dateList[2])+'_begin:'+"\n")
for item in threadExeList:
    f.write(str(item)+"\n")
f.write('Success:'+str(itemSuccessList[0])+"\n")
f.write('Failed:'+str(itemSuccessList[1])+"\n")
f.write("END"+"\n")
f.close()

