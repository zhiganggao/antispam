import sys
import MySQLdb
import time
import threading
import re
import gc
############
setList = ('user_agent','ip','user_id')
record = {}
xList = {}
date = time.time()-86400
dateList = time.localtime(date)
#############
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
writePath = "/home/brdwork/antispider/data/detail/detail-"+dateStr

###########

class dbAccess(object):  
    __h = '172.16.0.188'
    __u = 'mlsreader'
    __p = 'RMlSxs&^c6OpIAQ1'
    __d = 'test'
    __port = 3316
    __db = None
    __cursor = None
    def __init__(self):
	f = open('/home/work/conf/mysql/brd.mysql.ini')
	x = f.readlines();
	for item in x:
	    if 'db=test_brd_cpc' in item and 'master=0' in item:
		strs = item.split(" ")
		for value in strs:
		    if 'host' in value:
			self.__h = value[5:]
		    if 'port' in value:
			self.__port = int(value[5:])
		    if 'user' in value:
			self.__u = value[5:]
		    if 'pass' in value:
			self.__p = value[5:]
	self.__db = MySQLdb.connect(self.__h,self.__u,self.__p,self.__d,self.__port)
	self.__cursor = self.__db.cursor()	
	    
    def __new__(cls, *args, **kw):  
	if not hasattr(cls, '_instance'):  
	    orig = super(dbAccess, cls)  
	    cls._instance = orig.__new__(cls, *args, **kw) 
	return cls._instance
    def readT(self,dbName,column,where):
		
	sql = 'select '+','.join(column)+' from '+str(dbName)+' where '+str(where)
	try:
	    self.__cursor.execute(sql)    
	    results = self.__cursor.fetchall()	
	    return results
	except:
	    return ['error in sql:',sql]
	
    def writeT(self,dbName,column,value,muti = False):
	if muti:
	    for i in value:
		insertStrs = ''
		for item in i:
		    if 'str' in type(item).__name__:
			insertStrs += "'"+str(item)+"',"
		    else:
			insertStrs += str(item)+','
		insertStrs = insertStrs[0:-1]
		sql = 'insert into '+str(dbName)+'('+','.join(column)+') values('+insertStrs+")"
		try:
		    self.__cursor.execute(sql)
		    self.__db.commit()
		except:
		    self.__db.rollback()
		    return['error in sql:',sql]
	else:
	    insertStrs = ''
	    for item in value:
		if 'str' in type(item).__name__:
		    insertStrs += "'"+str(item)+"',"
		else:
		    insertStrs += str(item)+','
	    insertStrs = insertStrs[0:-1]
	    sql = 'insert into '+str(dbName)+'('+','.join(column)+') values('+insertStrs+")"
	    try:
		self.__cursor.execute(sql)
		self.__db.commit()
	    except:
		self.__db.rollback()
		return['error in sql:',sql]
	return ['success']
    #############
    
def strCompare(str1,str2):
    if len(str1) == 0 and len(str2) == 0:
	return -1
    max_len = 0
    max_len_list = {}
    devia = abs(len(str1)-len(str2))+1
    if not reduce(lambda x,y:x > y,(len(str1),len(str2))):
	str1,str2 = str2,str1
    is_set = False
    for index in range(0,len(str2)):
	max_len_list.setdefault(index,[])
	for index1 in range(0,len(str1)):
	    if not str2[index] == str1[index1]:
		if index > 0 and index1 > 0:
		    max_past = max(max_len_list[index - 1][index1 - 1],max_len_list[index - 1][index1],max_len_list[index][index1 - 1])
		elif index > 0:
		    max_past = max_len_list[index-1][index1]
		elif index1 >0:
		    max_past = max_len_list[index][index1 -1]
		else:
		    max_past = 0
		max_len_list[index].append(max_past)
	    else:
		if index > 0 and index1 > 0:
		    max_len_list[index].append(max_len_list[index - 1][index1 - 1] + 1)
		else:
		    max_len_list[index].append(1)
    for x in max_len_list.values():
	max_len = reduce(lambda x,y:max(x,y),(max(x),max_len))
    return float(max_len)/max(len(str1),len(str2))
	
    
###################
def readList():
    global dateStr
    global xList
    dbreader = dbAccess()
    tidList = []
    yList = dbreader.readT('t_malicious_report_pc', ['twitter_id','pvs','ip_group','user_group'], "pvs > 300 and degree > 10 and createtime = '"+dateStr[:-3]+"'")
    for v in yList:
	xList.setdefault(v[0],{})
	xList[v[0]]['count'] = v[1]
	xList[v[0]]['group1'] = v[2]
	xList[v[0]]['group2'] = v[3]
	tidList.append(str(v[0]))
    return tidList
################
def splitWithLog(path,tidList):
    shows = {}
    f = open(path,'r')
    strs = list()
    tidStr = ''
    dic ={}
    global record
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
	    if 'user_id' in strs[index]:
		if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
		else:dic['user_id'] = strs[index+1][1:-1]
	    if 'user_agent' in strs[index]:
		if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
		else:dic['user_agent'] = strs[index+1][1:-1] 
	    if 'unit_price' in strs[index]:
		if len(strs[index+1]) == 1:dic['unit_price'] = strs[index+2] 
		else:dic['unit_price'] = strs[index+1][1:-1]		
	    if 'uri' in strs[index]:
		if len(strs[index+1]) == 1:dic['uri'] = strs[index+2] 
		else:dic['uri'] = strs[index+1][1:-1] 
	    if 'time' in strs[index]:
		dic['time'] = strs[index+1][1:-1]                
	else:
	    if not dic.get('tid',-1) == -1 and dic.get('tid', -1) in tidList and int(dic.get('channel_source' ,-1)) >> 30 == 1:
		shows.setdefault(dic.get('tid', -1),[])
		shows[dic.get('tid', -1)].append(dic)
	    dic = {}
    for k,v in shows.items():
	record.setdefault(k, [])
	record[k].extend(v)
    f.close() 
###################
    
def writeDetail():
    global record
    global writePath
    for k,v in record.items():
	f = open(writePath+"-"+str(k),'w+')
	f.write("$$$$start$$$$\n")
	for dic in v:
	    writeStr = ''
	    for xset,value in dic.items():
		writeStr += str(xset)+":    "+str(value)+"\n"
	    f.write(writeStr)
	    f.write("$$$$end$$$$\n")
	f.flush()
	f.close()

##################
def group():
    global record
    global setList
    recordCollection = {}
    for k,v in record.items():
	recordCollection.setdefault(k,{})
	for setL in setList:
	    recordCollection[k].setdefault(setL,{})
	recordCollection[k].setdefault('uri',[])
	recordCollection[k].setdefault('unit_price',[])
	recordCollection[k].setdefault('refer',{})
	recordCollection[k].setdefault('ilreferoragent',[])
	index = 0
	for v1 in v:
	    for key,value in v1.items():
		if key in setList:
		    recordCollection[k][key].setdefault(value,[])
		    recordCollection[k][key][value].append((index,v1['time']))
		if key == 'time':
		    continue
		if key == 'uri':
		    if not re.match(r'.*(search).*',value,re.M|re.I):
			recordCollection[k]['refer'].setdefault(value,[])
			recordCollection[k]['refer'][value].append(index)
		    else:
			searchKey = ''
			search_key_list1 = value.split("?")
			search_key_list2 = search_key_list1[1].split('&')
			for search_key in search_key_list2:
			    if 'searchKey' in search_key:
				searchKey = search_key[10:]
			keys = recordCollection[k]['refer'].keys()
			for x in keys:
			    if strCompare(x,searchKey) > 0.8:
				recordCollection[k]['refer'][x].append(index)
				break
			else:
			    recordCollection[k]['refer'].setdefault(searchKey,[])
			    recordCollection[k]['refer'][searchKey].append(index)
			
		    if (re.match(r'.*(winHttp).*',v1['user_agent'],re.M|re.I)) or (re.match(r'.*(com\:).*',value,re.M|re.I)):
			recordCollection[k]['ilreferoragent'].append((1,value))
		    else:
			recordCollection[k]['ilreferoragent'].append((0,value))
		    if re.match(r'.*(share).*(item).*',value,re.M|re.I):
			if re.match(r'.*(\?).*',value,re.M|re.I):
			    recordCollection[k][key].append(1)
			else:
			    recordCollection[k][key].append(0)
		    elif (re.match(r'.*(search).*',value,re.M|re.I)) or (re.match(r'.*(navigation).*',value,re.M|re.I) or (re.match(r'.*(guang).*',value,re.M|re.I))):
			recordCollection[k][key].append(2)
		    else:
		#	print 'xman'
			#print value
			recordCollection[k][key].append(-1)
		if key == 'unit_price':
		    if float(value) > 0:
			recordCollection[k][key].append(1)
		    else: 
			recordCollection[k][key].append(0)
	    index += 1
    return recordCollection
################
def calculate(recordCollection):
    global xList
    global setList
    affectList = {}
    db_writer = dbAccess()
    for k,v in recordCollection.items():
	affectList.setdefault('all' ,[])
	affectList.setdefault('cpc' ,[])
	affectList.setdefault('refererror' ,[])
	affectList.setdefault('refernojuage' ,[])
	affectList.setdefault('referaffect' ,[])
	affectList.setdefault('single' ,[])	
	k = int(k)
	agentGroup = float(len(v['user_agent'])/xList[k]['count'])
	
	for count_refer in v['refer'].values():
	    if float(len(count_refer))/xList[k]['count'] > 0.9:
		affectList['all'].extend(count_refer)
	if float(xList[k]['group1']) < 0.5 and (abs(float(xList[k]['group1']) - agentGroup) < 0.1 or agentGroup < 0.2):
	    for m in v['ip'].values():
		if len(m) > int(1.0/float(float(xList[k]['group1'])+0.01)):
		    for n in m:
			if affectList['all'].count(n[0]) < 1:
			    affectList['all'].append(n[0])
	indexil = 0
	for ilrefer in v['ilreferoragent']:
	    if ilrefer[0] == 1:
		affectList['all'].append(indexil)
		#print ilrefer[1]
	    indexil += 1
	maxset = {}
	for xset in setList:
	    maxset.setdefault(xset,(0,''))
	    for key,value in v[xset].items():
		if str(key) == '0' or  str(key) == '02:00:00:00:00:00' or  str(key) == '2130706433':
		    continue  
		if len(value) > maxset[xset][0]:
		    maxset[xset] = (len(value),key)
	for xset1,maxnum in maxset.items():
	    
	    if maxnum[0] > 50 and xset1 != 'user_agent':
		for x in v[xset1][maxnum[1]]:
		    if affectList['all'].count(x[0]) < 1:
			affectList['all'].append(x[0])
	    elif xset1 == 'user_agent' and maxnum[0] > (xList[k]['count']*0.8):
		for x in v[xset1][maxnum[1]]:
		    if affectList['all'].count(x[0]) < 1:
			affectList['all'].append(x[0])		
	for index in v['uri']:
	    if index == 0:
		affectList['refererror'].append(index)
	    if index == 1:
		affectList['single'].append(index)
	    if index == 2:
		affectList['refernojuage'].append(index)		
	for index in affectList['all']:	    
	    if v['uri'][index] == 2:
		affectList['referaffect'].append(index)
	for index in affectList['referaffect']:
	    if v['unit_price'][index] == 1:
		affectList['cpc'].append(index)
	column = ('twitter_id','pvs','ip_group','user_group','er_refer_count','single_count','af_refer_count','possible_malicious_count','af_count','af_cpc_count','createtime')
	value = (int(k),int(xList[k]['count']),float(xList[k]['group1']),float(xList[k]['group2']),int(len(affectList['refererror'])),int(len(affectList['single'])),\
	         int(len(affectList['refernojuage'])),int(len(affectList['all'])),int(len(affectList['referaffect'])),int(len(affectList['cpc'])),str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
        print db_writer.writeT('t_malicious_result_pc_test', column, value)
	affectList = {}                                                                                                                                                
	    
		
####################
def splitLog(path,tidList):  
    excuteRecord = list() 
    for index in range(200):
	if len(str(index)) == 1:
	    num = "00"+str(index)
	else:
	    if len(str(index)) == 2:
		num = "0"+str(index)
	    else:num = str(index)
	try:    
	    splitWithLog(path+num,tidList)
	    excuteRecord.append(dateStr+str(index)+"completed\n")
	    print(dateStr+str(index)+"completed")
	except IOError:
	    continue
    gc.collect()
    return excuteRecord
	
if __name__ == '__main__':
    splitLog(path, readList())
    writeDetail()
    calculate(group())
    
