import sys
import time
import copy
if not "/home/brdwork/antispider/helper/" in sys.path:
    sys.path.append("/home/brdwork/antispider/helper/")
if not 'logCutHelper' in sys.modules:
    logCutHelper = __import__('logCutHelper')
else:
    eval('import logCutHelper')
    logCutHelper = eval('reload(logCutHelper)')
import multiprocessing
date = time.time()-86400
dateList = time.localtime(date)
path = "/home/brdwork/antispider/data/showsmap/range_mob-"

def splitWithTemp(path):
    db_operater = logCutHelper.dbAccess()   
    f = open(path,'r')
    tp1 = ['ip','user_id','imei','idfv','mac']
    strs = list()
    dic ={}
    key = 0
    dic.setdefault(0,{})
    for unit in f.readlines():
        strs.extend(unit.split(","))
    count = len(strs)
    time_avrege = {}
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
			    if int(collection[1])<= int(countIndex[1]):
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
	if 'maccount' in strs[index]:    
	    maccountIndex = strs[index].split(":")
	    dic[key].setdefault('maccount',0)
	    #print ipcountIndex
	    try:
		dic[key]['maccount'] += int(maccountIndex[1])  
	    except:
		dic[key]['maccount'] += 0
	if 'imeicount' in strs[index]:    
	    imeicountIndex = strs[index].split(":")
	    dic[key].setdefault('imeicount',0)
	    #print ipcountIndex
	    try:
		dic[key]['imeicount'] += int(imeicountIndex[1])  
	    except:
		dic[key]['imeicount'] += 0
	if 'idfvcount' in strs[index]:    
	    idfvcountIndex = strs[index].split(":")
	    dic[key].setdefault('idfvcount',0)
	    #print ipcountIndex
	    try:
		dic[key]['idfvcount'] += int(idfvcountIndex[1])  
	    except:
		dic[key]['idfvcount'] += 0	
	if 'totalcount' in strs[index]:    
	    totalcountIndex = strs[index].split(":")
	    dic[key].setdefault('totalcount',0)
	    #print ipcountIndex
	    try:
		dic[key]['totalcount'] += int(totalcountIndex[1])  
	    except:
		dic[key]['totalcount'] += 0		
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
	if 'imeicollection' in strs[index]:    
	    try:
		dic[key].setdefault('imeicollection',{})                
		imeiIndex = strs[index].split(":")
		imeiCollectionIndex = imeiIndex[1].split("|")
		for x in imeiCollectionIndex:
		    collection = x.split("-")
		    if any(collection):
			try:
			    dic[key]['imeicollection'].setdefault(collection[0],0)
			    dic[key]['imeicollection'][collection[0]] += int(collection[1])
			except:
			    pass
	    except:
		pass
	if 'idfvcollection' in strs[index]:    
	    try:
		dic[key].setdefault('idfvcollection',{})                
		idfvIndex = strs[index].split(":")
		idfvCollectionIndex = idfvIndex[1].split("|")
		for x in idfvCollectionIndex:
		    collection = x.split("-")
		    if any(collection):
			try:
			    dic[key]['idfvcollection'].setdefault(x[:-len(collection[-1])-1],0)
			    dic[key]['idfvcollection'][x[:-len(collection[-1])-1]] += int(collection[-1])
			except:
			    pass
	    except:
		pass
	    key = 0 
	if 'maccollection' in strs[index]:    
	    try:
		dic[key].setdefault('maccollection',{})                
		macIndex = strs[index][14:]
		macCollectionIndex = macIndex.split("|")
		for x in macCollectionIndex:
		    collection = x.split("-")
		    if any(collection):
			try:
			    dic[key]['maccollection'].setdefault(collection[0],0)
			    dic[key]['maccollection'][collection[0]] += int(collection[1])
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
	    outputItem.setdefault('twitter_id',value['tid'])
	    outputItem.setdefault('shows',value['count'])
	    outputItem.setdefault('createtime',str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
	    outputItem.setdefault('pc_rate',float(value['count'])/float(value['totalcount']))
	    for tp in tp1:
		outputItem.setdefault(tp+'_count',len(value[tp+'collection']))
		outputItem.setdefault(tp+'_total_count',value[tp+'count'])
		maxK = ''
		maxV = 0
		for k1,v1 in value[tp+'collection'].items():
		    if(v1 > maxV):
			if not (str(k1) == 'null' or str(k1) == '02:00:00:00:00:00' or str(k1) == '00:00:00:00:00:00'): 
			    maxV = v1
			    maxK = k1
		try:
		    if not value[tp+'count'] == 0:
			outputItem.setdefault(tp+'_group',round(float(len(value[tp+'collection']))/float(value[tp+'count']),2)*100)
			outputItem.setdefault(tp+'_max',str(maxK)+"|"+str(maxV))
		    else:
			outputItem.setdefault(tp+'_group',-1)
			outputItem.setdefault(tp+'_max',str(maxK)+"|"+str(-1))
		except:
			outputItem.setdefault(tp+'_group',-1)
			outputItem.setdefault(tp+'_max',str(maxK)+"|"+str(-1))
	    timeAvrege = [0.155,0.009,0.005,0.002,0.0008,0.0013,0.004,0.034,0.082,0.125,0.145,0.135,0.126,0.155,0.187,0.218,0.220,0.164,0.11,0.095,0.08,0.037,0.017,0.026]    
	    
	    time_avrege.setdefault('count',{})
	    time_avrege.setdefault('tongji',{})	    
	    for index in range(24):
		time_avrege['tongji'].setdefault(index,0)
		time_avrege['count'].setdefault(index,0)		
		if index != 23:
		    #timeCollect.setdefault(index,{})
		    outputItem.setdefault('time_deviation',0)
		#timeCollect[index].setdefault('count',0)
		    try:
			time_avrege['count'][index] += 1
			time_avrege['tongji'][index] += float(value['timecollection'].get(index,0)+value['timecollection'].get(index+1,0))/float(value['count'])
			
			outputItem['time_deviation'] += abs((float(value['timecollection'][index])+float(value['timecollection'][index+1]))/value['count'] - timeAvrege[index])
		    except:
			pass
		else:
		    try:
			time_avrege['count'][index] += 1
			time_avrege['tongji'][index] += float(value['timecollection'].get(index,0)+value['timecollection'].get(0,0))/float(value['count'])
			
			outputItem['time_deviation'] += abs((float(value['timecollection'][index])+float(value['timecollection'][0]))/value['count'] - timeAvrege[index])
		    except:
			pass
	    if len(db_operater.writeT('t_shows_log_analysis_mob_test',outputItem.keys(),outputItem.values())) > 1:
		print "error1"
	except:
	    print "error2"
	    continue
 #   for index in range(24):
#	print str(time_avrege['tongji'][index])+"     "+str(time_avrege['count'][index])+"    "+str(float(time_avrege['tongji'][index])/float(time_avrege['count'][index])) 
	 
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
