import sys
import time
import copy
import MySQLdb
import multiprocessing
date = time.time()-86400
dateList = time.localtime(date)
h = "172.16.0.188"
u = "mlsreader"
p = "RMlSxs&^c6OpIAQ1"
d = "test"
port = 3316
path = "/home/brdwork/antispider/data/showsmap/range-"

def splitWithTemp(path):
    global h,u,p,d,port
    db = MySQLdb.connect(h,u,p,d,port)
    cursor = db.cursor()    
    shows = list()
    record = {}
    f = open(path,'r')
    tp1 = ['ip','user_id']
    strs = list()
    dic ={}
    key = 0
    dic.setdefault(0,{})
    for unit in f.readlines():
        strs.extend(unit.split(","))
    count = len(strs)
    for index in range(count):
        #if "\n" not in strs[index]:                               
	if 'tid' in strs[index]:
	    tidSplit = strs[index].split(":")    
	    try:
		    key = int(tidSplit[1])
	    except:
		    key = 0
	    dic.setdefault(key,{})
	    dic[key]['tid'] = key
	if 'timecollection' in strs[index]:  
	    try:
		try:
		    countIndex = strs[index-1].split(":")
		    dic[key].setdefault('count',0)
		    dic[key]['count'] += int(countIndex[1])
		except:
		    dic[key].setdefault('count',0)
		    dic[key]['count'] += 0              
		dic[key].setdefault('timecollection',{})                
		timeIndex = strs[index].split(":")
		timeCollectionIndex = timeIndex[1].split("|")
		for x in timeCollectionIndex:
		    collection = x.split("-")
		    if any(collection):
			for hour in range(24):
			    dic[key]['timecollection'].setdefault(hour,0)
			try:
			    if int(collection[1])< int(countIndex[1]):
			    	dic[key]['timecollection'][int(collection[0])] += int(collection[1]) 
			except:
			    pass
	    except:
		pass
	if 'illegalcount' in strs[index]:
	    try:
		illegalcountIndex = strs[index].split(":")
		dic[key].setdefault('illegalcount',0)
		dic[key]['illegalcount'] += int(illegalcountIndex[1])
	    except:
		dic[key].setdefault('illegalcount',0)
		dic[key]['illegalcount'] += 0
	if 'ipcount' in strs[index]:    
	    ipcountIndex = strs[index].split(":")
	    dic[key].setdefault('ipcount',0)
	    #print ipcountIndex
	    try:
		dic[key]['ipcount'] += int(ipcountIndex[1])  
	    except:
		dic[key]['ipcount'] += 0
	if 'totalcount' in strs[index]:    
	    totalcountIndex = strs[index].split(":")
	    dic[key].setdefault('totalcount',0)
	    #print ipcountIndex
	    try:
		dic[key]['totalcount'] += int(totalcountIndex[1])  
	    except:
		dic[key]['totalcount'] += 0		
	    key = 0    
	if 'user_idcount' in strs[index]:    
	    user_idcountIndex = strs[index].split(":")
	    dic[key].setdefault('user_idcount',0)
	    try:
	        dic[key]['user_idcount'] += int(user_idcountIndex[1])
	    except:
	        dic[key]['user_idcount'] += 0
	 	
	if 'ipcollection' in strs[index]:    
	    try:
		dic[key].setdefault('ipcollection',{})                
		ipIndex = strs[index].split(":")
		ipCollectionIndex = ipIndex[1].split("|")
		for x in ipCollectionIndex:
		    collection = x.split("-")
		    if any(collection):
			try:
			    dic[key]['ipcollection'].setdefault(int(collection[0]),0)
			    dic[key]['ipcollection'][int(collection[0])] += int(collection[1])
			except:
			    pass
	    except:
		pass
	if 'user_idcollection' in strs[index]:   
	    try:
		dic[key].setdefault('user_idcollection',{})                
		user_idIndex = strs[index].split(":")
		user_idCollectionIndex = user_idIndex[1].split("|")
		for x in user_idCollectionIndex:
		    collection = x.split("-")
		    if any(collection):
			try:
			    dic[key]['user_idcollection'].setdefault(int(collection[0]),0)
			    dic[key]['user_idcollection'][int(collection[0])] += int(collection[1])                        
			except:
			    pass
	    except:
		pass
    f.close() 
    #print(dic)
    #timeCollect = {}
    for tid,value in dic.items():
	outputItem = {}
	try:
	#if True and value != {}:
	    outputItem.setdefault('tid',value['tid'])
	    outputItem.setdefault('shows',value['count'])
	    outputItem.setdefault('date',str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
	    outputItem.setdefault('ipcount',value['ipcount'])
	    outputItem.setdefault('pcrate',float(value['count'])/float(value['totalcount']))
	    outputItem.setdefault('user_idcount',value['user_idcount'])
	    outputItem.setdefault('ipnumbers',len(value['ipcollection']))
	    outputItem.setdefault('user_idnumbers',len(value['user_idcollection']))
	    for tp in tp1:
		maxK = ''
		maxV = 0
		for k1,v1 in value[tp+'collection'].items():
		    if(v1 > maxV):
	    
			maxV = v1
			maxK = k1
		try:
		    if not value[tp+'count'] == 0:
			outputItem.setdefault(tp+'Group',round(float(len(value[tp+'collection']))/float(value[tp+'count']),2)*100)
			outputItem.setdefault(tp+'Max',str(maxK)+"|"+str(maxV))
		    else:
			outputItem.setdefault(tp+'Group',-1)
			outputItem.setdefault(tp+'Max',str(maxK)+"|"+str(-1))
		except:
			outputItem.setdefault(tp+'Group',-1)
			outputItem.setdefault(tp+'Max',str(maxK)+"|"+str(-1))
	    timeAvrege = [0.155,0.009,0.005,0.002,0.0008,0.0013,0.004,0.034,0.082,0.125,0.145,0.135,0.126,0.155,0.187,0.218,0.220,0.164,0.11,0.095,0.08,0.037,0.017,0.026]    
	    for index in range(24):
		if index != 23:
		    #timeCollect.setdefault(index,{})
		    outputItem.setdefault('timeDeviation',0)
		#timeCollect[index].setdefault('count',0)
		    try:
			outputItem['timeDeviation'] += abs((float(value['timecollection'][index])+float(value['timecollection'][index+1]))/value['count'] - timeAvrege[index])
		    except:
			pass
		else:
		    try:
			outputItem['timeDeviation'] += abs((float(value['timecollection'][index])+float(value['timecollection'][0]))/value['count'] - timeAvrege[index])
		    except:
			pass
	    try:
	 #   if True:
		sql = "INSERT INTO t_shows_log_analysis_pc_test(twitter_id,shows,ip_total_count,ip_count,ip_group,ip_max,user_id_total_count,user_id_count,user_id_group,user_id_max,time_deviation,createtime,illegal_user_agent_num,pc_rate) VALUES ('%d','%d','%d','%d','%3.2f','%s','%d','%d','%3.2f','%s','%3.2f','%s','%d','%5.2f')" % (int(outputItem['tid']),int(outputItem['shows']),int(outputItem['ipcount']),int(outputItem['ipnumbers']),float(outputItem['ipGroup']),str(outputItem['ipMax']),int(outputItem['user_idcount']),int(outputItem['user_idnumbers']),float(outputItem['user_idGroup']),str(outputItem['user_idMax']),float(outputItem['timeDeviation']),str(outputItem['date']),0,float(outputItem['pcrate']))
		cursor.execute(sql)
		db.commit()
	    except:
		db.rollback()
	except:
	    print "error2"
	    continue
    db.close()
def excute(path,range_x):
    for index in range(range_x[0],range_x[1]):
	if index < 10:
	    fpath = path + "000"+str(index)
	elif index < 100:
	    fpath = path + "00"+str(index)
	elif index < 1000:
	    fpath = path + "0"+str(index)
	else:
	    fpath = path + str(index)
	splitWithTemp(fpath)
	print fpath

if __name__ == '__main__':
    record = []
    for i in range(5):
	process = multiprocessing.Process(target=excute,args=(path,[i*200,(i+1)*200]))
	process.start()
	record.append(process) 
    process = multiprocessing.Process(target=excute,args=(path,[1000,1013]))
    process.start()
    record.append(process)    
    for process in record:
	process.join()
    for index in range(1013):
	if index < 10:
	    fpath = path + "000"+str(index)
	elif index < 100:
	    fpath = path + "00"+str(index)
	elif index < 1000:
	    fpath = path + "0"+str(index)
	else:
	    fpath = path + str(index)
	f3 = open(fpath,"w")
	f3.close()
