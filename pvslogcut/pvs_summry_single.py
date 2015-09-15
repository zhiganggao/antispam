import sys
import MySQLdb
import time
import threading
import re
import gc
import string
############
setList = ('mac_addr','imei','user_agent','ip','user_id')
record = {}
date = time.time()-9*86400
dateList = time.localtime(date)
"""f = open('/home/brdwork/list1','r')
list1 = f.readlines()
list2 = []
for x in list1:
    m = str(x).split('|')
    list2.append(m)
twilist = {}
for y in list2:
    twilist.setdefault(y[0],[])
    twilist[y[0]].append(y[1])
for k,v in twilist.items():
    if k == str(dateList[0])+"-0"+str(dateList[1])+"-"+str(dateList[2]):
	juageTid = v
"""
juageTid = ['3686285341']

isTest = False
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
writePath = "/home/brdwork/antispider/data/group/detail-"+dateStr

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
    if len(str1) ==0 and len(str2) ==0:
	return -1
    max_len = 0
    max_len_list = {}
    devia = abs(len(str1)-len(str2))+1
    is_left = reduce(lambda x,y:x > y,(len(str1),len(str2)))
    if not is_left:
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
################
def splitWithLog(path,tidList):
    shows = {}
    f = open(path,'r')
    strs = list()
    tidStr = ''
    for item in tidList:
	tidStr += str(item)
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
	    if 'imei' in strs[index]:
		if len(strs[index+1]) == 1:dic['imei'] = strs[index+2] 
		else:dic['imei'] = strs[index+1][1:-1] 
	    if 'mac_addr' in strs[index]:
		if len(strs[index+1]) == 1:dic['mac_addr'] = strs[index+2] 
		else:dic['mac_addr'] = strs[index+1][1:-1] 
	    if 'unit_price' in strs[index]:
		if len(strs[index+1]) == 1:dic['unit_price'] = strs[index+2] 
		else:dic['unit_price'] = strs[index+1][1:-1]		
	    if 'uri' in strs[index]:
		if len(strs[index+1]) == 1:dic['uri'] = strs[index+2] 
		else:dic['uri'] = strs[index+1][1:-1] 
	    if 'time' in strs[index]:
		dic['time'] = strs[index+1][1:-1]                
	else:
	    if not dic.get('tid',-1) == -1 and dic.get('tid', -1) in tidStr:
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
    print record
    for k,v in record.items():
	f = open(writePath+"-"+str(k)+'.csv','w+')
	index = 0
	for dic in v:
	    writeStr = str(index)+','
	    for xset,value in dic.items():
		if xset == 'channel_source':
		    value = str(int(value) >> 30)
		value = value.replace(',','')
		value = value.replace('\\','')
		writeStr +=str(value)+","
	    f.write(writeStr)
	    f.write('\n')
	    index += 1
	f.flush()
	f.close()

##################
def group():
    global record
    global setList
    global writePath
    f = open('/home/brdwork/antispider/helper/mac_list','r')
    mac_list = f.readlines()
    f.close()
    recordCollection = {}
    for k,v in record.items():
	recordCollection.setdefault(k,{})
	for setL in setList:
	    recordCollection[k].setdefault(setL,{})
	recordCollection[k].setdefault('uri',[])
	recordCollection[k].setdefault('unit_price',[])
	recordCollection[k].setdefault('refer',{})
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
			if int(v1['channel_source'])>>30 == 2:
			    search_key_list1 = value.split(":")
			    for search_key in search_key_list1:
				if 'tag_word' in search_key:
				    searchKey = search_key[9:]
			else:
			    search_key_list1 = value.split("?")
			    search_key_list2 = search_key_list1[1].split('&')
			    for search_key in search_key_list2:
				if 'searchKey' in search_key:
				    searchKey = search_key[10:]
			keys = recordCollection[k]['refer'].keys()
			for x in keys:
			    if strCompare(x,searchKey) > 0.9:
				recordCollection[k]['refer'][x].append(index)
				break
			else:
			    recordCollection[k]['refer'].setdefault(searchKey,[])
			    recordCollection[k]['refer'][searchKey].append(index)
		    
		    if re.match(r'.*(winHttp).*',v1['user_agent'],re.M|re.I):
			recordCollection[k][key].append(0)
		    elif re.match(r'.*(com:).*',value,re.M|re.I):
			recordCollection[k][key].append(0)
		    elif re.match(r'.*(share).*(item).*',value,re.M|re.I):
			if re.match(r'.*(\?).*',value,re.M|re.I):
			    recordCollection[k][key].append(1)
			else:
			    recordCollection[k][key].append(0)
		    elif re.match(r'.*(search).*',value,re.M|re.I) or re.match(r'.*(navigation).*',value,re.M|re.I or (re.match(r'.*(catalog).*',value,re.M|re.I) and not re.match(r'.*(com:).*',value,re.M|re.I))):
			recordCollection[k][key].append(2)
		    else:
			recordCollection[k][key].append(-1)
		if key == 'unit_price':
		    if value > 0:
			recordCollection[k][key].append(1)
		    else: 
			recordCollection[k][key].append(0)
	    index += 1
    ilmac = 0
    mi = 0
    for tid,io in recordCollection.items():
	f = open(writePath+"-"+str(tid)+'_group.csv','w+')
	for kkkk,mmmm in sorted(io['ip'].items(),key = lambda tid:tid[0]):
	    f.write("ip:"+str(kkkk)+": count:"+str(len(mmmm)) + "\n")
	    for xxxx in mmmm:
 	    	f.write(str(xxxx).replace('(','').replace(')','') + '\n')
	for kkkk,mmmm in io['refer'].items():
	    f.write("requri:"+str(kkkk)+": count:"+str(len(mmmm)) + "\n")
	    for xxxx in mmmm:
 	    	f.write(str(xxxx).replace('(','').replace(')','') + '\n')
	for kkkk1,mmmm1 in io['mac_addr'].items():
	    macX = "".join(kkkk1.split(":"))[0:6]
	    if macX.upper()+"\n" in mac_list:
		mi += len(mmmm1)
	    elif not (macX == '000000' or macX == '020000' or macX == 'null'):
		ilmac += len(mmmm1)
	f.write(str(ilmac))
	f.write(str(mi))
	f.close()
    return recordCollection
################

def splitLog(path,tidList):  
    excuteRecord = list() 
    global isTest
    if isTest:
	rangeNum = 2
    else:
	rangeNum = 200
    for index in range(rangeNum):
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
    splitLog(path, juageTid)
    writeDetail()
    group()
    
