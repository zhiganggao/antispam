import sys
import time
import re
import json
import os
reload(sys)
sys.setdefaultencoding( "utf-8" )

if not "/home/brdwork/antispider/helper/" in sys.path:
    sys.path.append("/home/brdwork/antispider/helper/")
if not 'logCutHelper' in sys.modules:
    logCutHelper = __import__('logCutHelper')
else:
    eval('import logCutHelper')
    logCutHelper = eval('reload(logCutHelper)')
xList = {}   
dayPast = 1
dt = time.time() - dayPast*86400
dateList = time.localtime(dt)


if len(str(dateList[1]))>1:
    if len(str(dateList[2]))>1:
	dateStr = str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2])
    else:
	dateStr = str(dateList[0])+'-'+str(dateList[1])+'-0'+str(dateList[2])
else:
    if len(str(dateList[2]))>1:
	dateStr = str(dateList[0])+'-0'+str(dateList[1])+'-'+str(dateList[2])
    else:
	dateStr = str(dateList[0])+'-0'+str(dateList[1])+'-0'+str(dateList[2]) 
writePath = "/home/brdwork/antispider/data/detail/detail_"
	
setListMob = ('user_agent','ip','user_id','mac_addr','imei','idfv')
setListPc = ('user_agent','ip','user_id')
########################################################################
class logCut():
    """new logcut on pvs,use json"""
    __dt = ''
    __path = "/data/scribelog/brd_ad/cpc/brdad_cpc_all_tid_shop_pvs/"
    #----------------------------------------------------------------------
    def __init__(self,dt):
        
        """Constructor"""
        try:
            dateList = time.localtime(dt)
        except:
            print 'error date,please input a int'
            return
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
        self.__path += "brdad_cpc_all_tid_shop_pvs-"+dateStr
    #-----------------------------------------------------------------------
    def explode(self,startNum,endNum,twiList):
        """to cut the log"""
        twiStr = ''
        cutp = []
        cutd = {} 
	print sorted(twiList)
	print len(twiList)
        for twi in twiList:
            twiStr += str(twi) + "|"        
        try:
            startNum = int(startNum)
            endNum = int(endNum)
        except:
            print 'please input a right startNum||endNum!'
            return
        for num in range(startNum,endNum):
    #        try:
	    cutp = self.__explodeDetail(num,twiStr)
	    for key,logslices in cutp.items():  
		for logslice in logslices:
		    cutd.setdefault(logslice['tid'],{})
		    cutd[logslice['tid']].setdefault(key,[])
		    cutd[logslice['tid']][key].append(logslice)                   
     #       except:
      #          continue
        return cutd
    
    #-----------------------------------------------------------------------
    def inplode(self,convertList):
        """convert to json just for test"""
        
        for convertobject in convertList:
            cutp = self.__inplodeDetail(convertobject)
        
            
    #-----------------------------------------------------------------------
    def __inplodeDetail(self,anyObject):
        """convert to json just for test"""
        try:
            json.dumps(anyObject)
        except:
            print "this cannot be convert"
            
    #-----------------------------------------------------------------------
    def __explodeDetail(self,num,twiStr):
        """pass"""
        numStr = ''
        path = ''
        fLog = None
        jsonexplodecode = {}
        resultList = {}
	resultList.setdefault('pc',[])
	resultList.setdefault('mob',[])
        if len(str(num)) == 1:
            numStr = '000'+str(num)
        elif len(str(num)) == 2:
            numStr = '00'+str(num)
        elif len(str(num)) == 3:
            numStr = '0'+str(num)
        else:
            numStr = str(num)
        path = self.__path + numStr
        
        try:
            fLog = open(path,'r')
        except:
            print 'path error,please check!'
            return resultList
        print path+'-'+str(num)+'running'
        for logItem in fLog.readlines():
	    if len(logItem) < 40:
		continue
	    index = {}
	    index.setdefault('b',0)
	    index.setdefault('e',-1)
	    for num in range(0,len(logItem)):
		if '{' == logItem[num]:
		    index['b'] = num
		    break
	    for num in range(len(logItem)-1,0,-1):
		if '}' == logItem[num]:
		    index['e'] = num
		    break

	    jsonCode = logItem[index['b']:index['e']+1]
	    try:
		jsonexplodecode = json.loads(jsonCode)
	    except:
		print jsonCode
		print logItem	
		#print logItem
		#print jsonexplodecode
	    if jsonexplodecode['tid'] in twiStr:
		if int(jsonexplodecode['channel_sources']) >> 30 == 2: 
		    resultList['mob'].append(jsonexplodecode)
		if int(jsonexplodecode['channel_sources']) >> 30 == 1:
		    resultList['pc'].append(jsonexplodecode)
        return resultList
    
#############################################

def getBlackList():
    setList = ('mob_ip','mob_user_id','idfv','imei','mac_addr','pc_ip','pc_user_id')
    dbreader = logCutHelper.dbAccess()
    global dateList
    list1 = dbreader.readT('t_pvs_refer_group',['mob_top10_ip','mob_top10_user_id','mob_top10_idfv','mob_top10_imei','mob_top10_mac_addr','pc_top10_ip','pc_top10_user_id'],"createtime = '"+str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2])+"'")
    xList = {}
    for item in setList:
	xList.setdefault(item,{})
    for item in list1:
	for num in range(0,7):
	    if num == 2:
		split = item[num]
		splitList = split.split('|')
		for item1 in splitList:
		    if len(item1[:-(len(item1.split('-')[-1])+1)]) < 2:
			continue
		    xList[setList[num]].setdefault(item1[:-(len(item1.split('-')[-1])+1)],0)
		    try:
			xList[setList[num]][item1[:-(len(item1.split('-')[-1])+1)]] += int(item1.split('-')[-1])
		    except:
			print item1
		continue

	    split = item[num]
	    splitList = split.split('|')
	    for item1 in splitList:
		xList[setList[num]].setdefault(item1.split('-')[0],0)
		try:
		    xList[setList[num]][item1.split('-')[0]] += int(item1.split('-')[1])
		except:
		    pass
    returnList = {}
    for key,value in xList.items():
	value = sorted(value.items(),key = lambda x:x[1],reverse=True)
	returnList.setdefault(key,[])
	for m in value:
	    if m[1] > 100:
		returnList[key].append(m[0])
    for key,item in returnList.items():
	for x in item:
	    if len(dbreader.readT('t_malicious_black_list',['id'],"where type = '"+str(key)+"' and value = '"+str(x)+"'")) < 1:
		print dbreader.writeT('t_malicious_black_list',['type','value'],[str(key),str(x)])
    y = dbreader.readT('t_malicious_black_list',['type','value'])
    returnList = {}
    for item in y:
	returnList.setdefault(item[0],[])
	returnList[item[0]].append(item[1])
    return returnList



#############################################
    
def readList():
    global xList
    dbreader = logCutHelper.dbAccess()
    pList = []
    yList = dbreader.readT('t_malicious_report_mob', ['twitter_id','ip_group','user_id_group','mac_addr_group','imei_group','idfv_group','degree'], "((pvs > 250 and degree > 8) or degree >= 40) and createtime = '"+dateStr+"'")
    yList2 = dbreader.readT('t_malicious_report_pc', ['twitter_id','ip_group','user_id_group','degree'], "((pvs > 100 and degree > 10) or degree >= 40) and createtime = '"+dateStr+"'")
    yList = list(yList)
    yList2 = list(yList2)
    for twi in yList:
        pList.append(twi[0])
	xList.setdefault(str(twi[0]),{})
	xList[str(twi[0])].setdefault('mob',twi)
	if twi[-1] > 50:
	    text = 'high degree warning:mob:'
	    text = str(twi)
	    for xstr in twi:
		text += str(xstr)+' '
	    os.system(" echo "+text+" | mail -s "+text+" zhiganggao@meilishuo.com")
    for twi in yList2:
	pList.append(twi[0])
	xList.setdefault(str(twi[0]),{})
	xList[str(twi[0])].setdefault('pc',twi)
	if twi[-1] > 50:
	    text = 'high degree warning:pc:'
	    for xstr in twi:
		text += str(xstr)+' '
	    os.system(" echo "+text+" | mail -s "+text+" zhiganggao@meilishuo.com")
    return pList

#############################################
def writeDetail(record):
    global writePath
    global dateStr
    for tid,detail in record.items():
	try:
	    value = detail['pc']
	    f = open(writePath+'pc-'+str(dateStr)+'_'+str(tid)+'.csv', 'w+')
	    for item in value:
		f.write(str(item)+'\n')
	    f.flush()
	    f.close()
	except:
	    pass
	try:
	    value = detail['mob']
	    f = open(writePath+'mob-'+str(dateStr)+'_'+str(tid)+'.csv', 'w+')
	    for item in value:
		f.write(str(item)+'\n')
	    f.flush()
	    f.close()	    
	except:
	    pass
###############

def group(record,setList):
    str_compare = logCutHelper.strsCompare()
    recordCollection = {}
    for setL in setList:
	recordCollection.setdefault(setL,{})
    recordCollection.setdefault('uri',[])
    recordCollection.setdefault('unit_price',[])
    recordCollection.setdefault('refer',{})
    recordCollection.setdefault('ilreferoragent',[])
    recordCollection.setdefault('totalcount',len(record))
    index = 0
    for v1 in record:
	for key,value in v1.items():
	    if key in setList:
		recordCollection[key].setdefault(value,[])
		recordCollection[key][value].append((index,v1['time']))
	    if key == 'time':
		continue
	    if key == 'uri':
		if not re.match(r'.*(search).*',value,re.M|re.I):
		    recordCollection['refer'].setdefault(value,[])
		    recordCollection['refer'][value].append(index)
		else:
		    search_key_list1 = value.split(":")
		    for search_key in search_key_list1:
			if 'tag_word' in search_key:
			    searchKey = search_key[9:]
			else:
			    searchKey = 'unknown'
		    keys = recordCollection['refer'].keys()
		    for x in keys:
			if float(len(x))/float(len(searchKey)) > 0.9 and float(len(x))/float(len(searchKey)) < 1.1:
			    if str_compare.strCompare(x,searchKey) > 0.8:
				recordCollection['refer'][x].append(index)
				break
		    else:
			recordCollection['refer'].setdefault(searchKey,[])
			recordCollection['refer'][searchKey].append(index)
		
		try:
		    if (re.match(r'.*(winHttp).*',v1['user_agent'],re.M|re.I)) or (re.match(r'.*(com\:).*',value,re.M|re.I)):
			recordCollection['ilreferoragent'].append((1,value))
		    else:
			recordCollection['ilreferoragent'].append((0,value))
		except:
		    recordCollection['ilreferoragent'].append((1,value))
		if re.match(r'.*(share).*(item).*',value,re.M|re.I):
		    if re.match(r'.*(\?).*',value,re.M|re.I):
			recordCollection[key].append(1)
		    else:
			recordCollection[key].append(0)
		elif (re.match(r'.*(search).*',value,re.M|re.I)) or (re.match(r'.*(navigation).*',value,re.M|re.I) or (re.match(r'.*(guang).*',value,re.M|re.I))):
		    recordCollection[key].append(2)
		else:
		    recordCollection[key].append(-1)
	    if key == 'unit_price':
		if float(value) > 0:
		    recordCollection[key].append(1)
		else: 
		    recordCollection[key].append(0)
	index += 1
    return recordCollection
################################

################################
def calculate(k,recordCollection,setList,blackList):
    """to calculate the true number"""
    k = str(k)
    f2 = open('/home/brdwork/antispider/helper/mac_list','r')
    mac_list = f2.readlines() 
    f2.close()
    global dateStr
    global xList
    #----------------------------
    affectList = {}
    reasonList = {}
    resultList = []
    affectList.setdefault('all' ,[])
    affectList.setdefault('cpc' ,[])
    affectList.setdefault('refererror' ,[])
    affectList.setdefault('refernojuage' ,[])
    affectList.setdefault('referaffect' ,[])
    affectList.setdefault('single' ,[])	
    agentGroup = len(recordCollection['user_agent'])/int(recordCollection['totalcount'])
    
    reasonList.setdefault('referMax',[])
    reasonList.setdefault('ilRefer',[])	
    reasonList.setdefault('agent',[])	
    reasonList.setdefault('ilMac',[])
       
    for xset in setList:
	if not (xset == 'ip' or xset == 'user_id' or xset == 'user_agent'): 
	    for key,value in recordCollection[xset].items():
		if str(key) in blackList[xset]:
		    reasonList.setdefault('blackList_'+str(xset),[])
		    for n in value:
			reasonList['blackList_'+str(xset)].append(n[0])
			if affectList['all'].count(n[0])<1:
			    affectList['all'].append(n[0])
	elif len(setList) < 5 and not xset == 'user_agent':
	    for key,value in recordCollection[xset].items():
		if str(key) in blackList['pc_'+ xset]:
		    reasonList.setdefault('blackList_'+str(xset),[])
		    for n in value:
			reasonList['blackList_'+str(xset)].append(n[0])
			if affectList['all'].count(n[0])<1:
			    affectList['all'].append(n[0])
	elif not xset == 'user_agent':
	    for key,value in recordCollection[xset].items():
		if str(key) in blackList['mob_'+xset]:
		    reasonList.setdefault('blackList_'+str(xset),[])
		    for n in value:
			reasonList['blackList_'+str(xset)].append(n[0])
			if affectList['all'].count(n[0])<1:
			    affectList['all'].append(n[0])
    for count_refer in recordCollection['refer'].values():
	if float(len(count_refer))/recordCollection['totalcount'] > 0.85:
	    reasonList['referMax'].extend(count_refer)
	    #affectList['all'].extend(count_refer)
    indexil = 0
    for ilrefer in recordCollection['ilreferoragent']:
	if ilrefer[0] == 1:
	    reasonList['ilRefer'].append(indexil)
	    if affectList['all'].count(indexil) < 1:
		affectList['all'].append(indexil)
		    #print ilrefer[1]
	indexil += 1			
    maxset = {}
    for index in range(1,len(setList)):
	if 'user_agent' == setList[index]:
	    continue
	try:
	    if len(setList) == 6:
		if float(xList[k]['mob'][index]) < 0.5 and (abs(float(xList[k]['mob'][index]) - agentGroup) < 0.1 or agentGroup < 0.2):
		    for m in recordCollection[setList[index]].values():
			print 'juagegroup'
			if len(m) > int(1.0/float(xList[k]['mob'][index])+0.01):
			    for n in m:
				reasonList.setdefault(setList[index]+'groupError',[])
				reasonList[setList[index]+'groupError'].append(n[0])
				if affectList['all'].count(n[0]) < 1:
				    affectList['all'].append(n[0]) 
	    else:
		if float(xList[k]['pc'][index]) < 0.5 and (abs(float(xList[k]['pc'][index]) - agentGroup) < 0.1 or agentGroup < 0.2):
		    for m in recordCollection[setList[index]].values():
			print 'juagegroup'
			if len(m) > int(1.0/float(xList[k]['pc'][index])+0.01):
			    for n in m:
				reasonList.setdefault(setList[index]+'groupError',[])
				reasonList[setList[index]+'groupError'].append(n[0])
				if affectList['all'].count(n[0]) < 1:
				    affectList['all'].append(n[0]) 
	except:
	    print 'setList num error'
    for xset in setList:
	maxset.setdefault(xset,(0,''))
	if not 'ip' == xset:
	    for key,value in recordCollection[xset].items():
		if str(key) == 'null' or str(key) == '0' or  str(key) == '02:00:00:00:00:00' or  str(key) == '00:00:00:00:00:00' or str(key) == '2130706433' or str(key) == 'None' or str(key) == '':
		    continue  
		if len(value) > maxset[xset][0]:
		    maxset[xset] = (len(value),key)
	else:
	    ipcollection = {}
	    for ip in sorted(recordCollection['ip'].keys()):
		if ip == '':
		    ip = 0
		for key,mark in ipcollection.items():
		    if abs(int(key) - int(ip)) < 10:
			ipcollection[ip] = mark
			break;
		else:
		    ipcollection[ip] = ip
	    distinct_ipcollection = {}
	    for key,mark in ipcollection.items():
		distinct_ipcollection.setdefault(mark,0)
		if key == 0:
		    key = ''
		distinct_ipcollection[mark] += len(recordCollection['ip'][key])		
	    maxK = ''
	    maxV = 0
	    for k1,v1 in distinct_ipcollection.items():
		if(v1 > maxV):
		    maxV = v1
		    maxK = k1
	    maxset[xset] = (maxV,maxK)

    for xset1,maxnum in maxset.items():

	if maxnum[0] > 50 and xset1 != 'user_agent':
	    for x in recordCollection[xset1][maxnum[1]]:
		reasonList.setdefault(str(xset1)+'max',[]) 
		reasonList[str(xset1)+'max'].append(x[0])		
		if affectList['all'].count(x[0]) < 1:
		    affectList['all'].append(x[0])
	elif xset1 == 'user_agent' and maxnum[0] > (recordCollection['totalcount']*0.8):
	    for x in recordCollection[xset1][maxnum[1]]:
		reasonList['agent'].append(x[0])
		if affectList['all'].count(x[0]) < 1:
		    affectList['all'].append(x[0])    
    for index in recordCollection['uri']:
	if index == 0:
	    affectList['refererror'].append(index)
	if index == 1:
	    affectList['single'].append(index)
	if index == 2:
	    affectList['refernojuage'].append(index)		
    for index in affectList['all']:
	if recordCollection['uri'][index] == 2:
	    affectList['referaffect'].append(index)
    for index in affectList['referaffect']:
	if recordCollection['unit_price'][index] == 1:
	    affectList['cpc'].append(index)  
    indexToDic = {}
    reasonList1 = {}
    for key,list1 in reasonList.items():
	for item in list1:
	    reasonList1.setdefault(item,[])
	    reasonList1[item].append(key)
    reasonList = {}
    for key,item in recordCollection['ip'].items():
	#for key,values in item.items():
	for value in item:
	    indexToDic.setdefault(value[0],[value[1],str(key)])
    for index in affectList['referaffect']:
	indexToDic[index].append(reasonList1[index])
	resultList.append(indexToDic[index])
    return resultList
def getFeeList(dateStr,tid):
    try:
	f = open('/home/brdwork/antilist/'+dateStr + '-' + str(tid)+'.csv','r')
    except:
	return []
    feeList = f.readlines()
    f.close()
    ipList = []
    for fee in feeList:
	feeSplit = json.loads(fee)
	dateSplit = feeSplit[-5].split('-')
	timeTuple = (dateSplit[0],dateSplit[1],dateSplit[2],feeSplit[-4],feeSplit[-3],feeSplit[-2])
	ip = str(feeSplit[1])
	if '.' in ip:
	    ipSplit = ip.split('.')
	    ip = 0
	    index = 0
	    for num in ipSplit:
		ip += int(num)*pow(2,24-index*8)
		index += 1
	ip = str(ip)
	price = str(feeSplit[2])
	operationTime = str(time.mktime(time.strptime(str(timeTuple[0])+' '+str(timeTuple[1])+' '+str(timeTuple[2])+' '+str(timeTuple[3])+' '+str(timeTuple[4])+' '+str(timeTuple[5]), "%Y %m %d %H %M %S")))[:-2]
	ipList.append((operationTime,ip,price))
    return ipList
def dataWrite(dateStr,tid,errorList):
    global xList
    feeList = getFeeList(dateStr, tid)
    feeListDic = {}
    count = 0 
    amount = 0.0
    reasonList = {}
    reasonFeeList = {}
    degreeList = {}
    
    
    try:
	degreeList.setdefault('pc','')
	degreeList['pc'] = str(xList[str(tid)]['pc'][-1])
    except:
	pass
    try:
	degreeList.setdefault('mob','')
	degreeList['mob'] = str(xList[str(tid)]['mob'][-1])
    except:
	pass    
    for fee in feeList:
	feeListDic.setdefault(fee[0]+'|'+fee[1],float(fee[2]))
    for error in errorList:
	if feeListDic.has_key(str(error[0])+'|'+str(error[1])):
	    amount += feeListDic[str(error[0])+'|'+error[1]]
	    reasonList.setdefault(reduce(lambda x,y:str(x)+'&'+str(y),error[2]),0)
	    reasonList[reduce(lambda x,y:str(x)+'&'+str(y),error[2])] += 1
	    reasonFeeList.setdefault(reduce(lambda x,y:str(x)+'&'+str(y),error[2]),0)
	    reasonFeeList[reduce(lambda x,y:str(x)+'&'+str(y),error[2])] += feeListDic[str(error[0])+'|'+error[1]]	    
	    count += 1
    dbWriter = logCutHelper.dbAccess()
    reasonStr = ''
    reasonFeeStr = ''
    degreeStr = ''
    for key,value in degreeList.items():
	degreeStr += str(key)+':'+value+'|'
    degreeStr = degreeStr[:-1]
    for key,value in reasonList.items():
	reasonStr += str(key)+':'+str(value)+'|'
	reasonFeeStr += str(key) + ':' +str(reasonFeeList[key])+'|'
    reasonStr =reasonStr[:-1]
    reasonFeeStr =reasonFeeStr[:-1]
    print dbWriter.writeT('t_malicious_result',['twitter_id','affected_pvs','pvs','affected_fee','reason','reason_detail','degree','createtime'],(int(tid),count,len(feeList),amount,str(reasonStr),str(reasonFeeStr),str(degreeStr),dateStr))
    if amount > 120 or count >300 or (count > 200 and len(feeList) < 350):
	text = 'warning:'+str(tid) + '-' + dateStr + 'affected.'+'affect count:'+str(count)+'.'+'all count:'+str(len(feeList))+'.'+'amount:'+str(amount)+'.'+'reasonList:'+str(reasonStr).replace('|',' ').replace('&','+')
	os.system(" echo "+text+" | mail -s "+text+" zhiganggao@meilishuo.com")

if __name__ == '__main__':
    blackList = getBlackList()
    logcut = logCut(dt)
    allList = logcut.explode(0,100,readList())
    print len(allList)
    tidu =  allList.keys()
    tids = readList()
    print len(tids),len(tidu)
    i = 0
    for x in tids:
	for y in tidu:
	    if str(x) == str(y):
		print i
		break
	else:
	    print x
	i += 1
    writeDetail(allList)
    count = len(allList)
    ir = 0
    listPc = []
    for tid,platform in allList.items():
	ir += 1
	listPc = []
	ispc = 0
	ismob = 0
	try:
	    x = platform['mob']
	    ismob = 1
	except:
	    pass
	try:
	    x = platform['pc']
	    ispc = 1
	except:
	    pass
	if ismob:
	    listMob = calculate(tid,group(platform['mob'], setListMob),setListMob,blackList)
	if ispc:
	    listPc = calculate(tid,group(platform['pc'], setListPc),setListPc,blackList)
	listPc.extend(listMob)
	dataWrite(dateStr, tid, listPc)
	print 'calculate rate: '+str(round(float(ir)/count*100))+'%'
	
	    
    
     
    
    
