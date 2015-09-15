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
path = "/home/brdwork/antispider/data/showsmap/range-single-"
totalcount = 0
totalrefercollection = {}

def splitWithTemp(path):
    global h,u,p,d,port,totalcount,totalrefercollection
    #db = MySQLdb.connect(h,u,p,d,port)
    #cursor = db.cursor()    
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
		    totalcount += int(countIndex[1])
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
			    totalrefercollection.setdefault(int(collection[0]),0)
			    totalrefercollection[int(collection[0])] += int(collection[1]) 
			    dic[key]['user_idcollection'].setdefault(int(collection[0]),0)
			    dic[key]['user_idcollection'][int(collection[0])] += int(collection[1])                        
			except:
			    pass
	    except:
		pass
	if 'wordcollection' in strs[index]:   
	    try:
		dic[key].setdefault('wordcollection',{})                
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
	print value
	
    #db.close()
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
    print totalcount
    print totalrefercollection
    """for index in range(1013):
	if index < 10:
	    fpath = path + "000"+str(index)
	elif index < 100:
	    fpath = path + "00"+str(index)
	elif index < 1000:
	    fpath = path + "0"+str(index)
	else:
	    fpath = path + str(index)
	f3 = open(fpath,"w")
	f3.close()"""
