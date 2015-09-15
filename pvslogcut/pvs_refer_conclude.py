import sys
import time
import re
import json
import os
import copy
reload(sys)
sys.setdefaultencoding( "utf-8" )

if not "/home/brdwork/antispider/helper/" in sys.path:
    sys.path.append("/home/brdwork/antispider/helper/")
if not 'logCutHelper' in sys.modules:
    logCutHelper = __import__('logCutHelper')
else:
    eval('import logCutHelper')
    logCutHelper = eval('reload(logCutHelper)')
totalcount = 0
xList = {}   
dayPast = 11
dt = time.time() - dayPast*86400
dateList = time.localtime(dt)
print dateList


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
mapPath = '/home/brdwork/antispider/data/pvsmap/range_'
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
    def explode(self,startNum,endNum):
        """to cut the log"""
        cutp = []
        cutd = {}        
	trueendNum = 0
        try:
            startNum = int(startNum)
            endNum = int(endNum)
        except:
            print 'please input a right startNum||endNum!'
            return
	for num in range(startNum,endNum):
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
		open(path,'r')
		trueendNum =  num
	    except:
		continue
        for num in range(startNum,trueendNum):
            self.__explodeDetail(num)
	    print 'Map running:' + str(round(float(num-startNum+1)/float(trueendNum-startNum)*100,3))+'%'
            

    #-----------------------------------------------------------------------
    def inplode(self,convertList):
        """convert to json just for test"""

        for convertobject in convertList:
            cutp = self.__inplodeDetail(convertobject)


    #-----------------------------------------------------------------------
    def __inplodeDetail(self,anyObject):
        """convert to json just for test"""
        try:
            return json.dumps(anyObject)
        except:
            return "this cannot be convert"

    #-----------------------------------------------------------------------
    def __explodeDetail(self,num):
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
            return resultList
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
		pass
            try:
		tag_word = jsonexplodecode['tag_word']
	    except:
		tag_word = str(jsonexplodecode['key_id'])
	    calculateNum = 0
	    markNum = 1
	    index = 0
	    for char in tag_word:
		if index  == 3:
		    markNum = ord(char)
		calculateNum += ord(char)
		index += 1

	    calculateNum *= len(tag_word)* markNum
	    rangenum = calculateNum % 1013
            f = open('/home/brdwork/antispider/data/pvsmap/range_'+str(rangenum),'a+')
            f.write(self.__inplodeDetail(jsonexplodecode)+'\n')
            f.close()
        
def pvsReduce(num):
    global mapPath
    num = str(num)
    try:
	f = open(mapPath+num,'r')
    except:
	return {}
    allList = f.readlines()
    f.close()
    cutd = {}
    for item in allList:
	item = json.loads(item)
	if int(item['channel_sources']) >>30 == 1:
	    platform = 'pc'
	else:
	    platform = 'mob'
	try:
	    item['tag_word'] = str(item['tag_word'])
	    if len(item['tag_word']) < 2:
		continue
	    cutd.setdefault(item['tag_word'],{})
	    cutd[item['tag_word']].setdefault(platform,[])
	    cutd[item['tag_word']][platform].append(item)
	except:
	    print item
	
    return cutd	
def pvsGroup(itemList):
    groupCollection = {}
    totalcount = {}
    totalcount.setdefault('pc' ,0)
    totalcount.setdefault('mob' ,0)
    for platform,detail in itemList.items():
        groupCollection.setdefault(platform,{})
        totalcount[str(platform)] += len(detail)
        for value in detail:
            for key1,value1 in value.items():
                if key1 == 'ip':
                    if str(value1) == 'None' or str(value1) == '' or str(value1) == 'null' or str(value1) == '2130706433':
                        continue                    
                    groupCollection[platform].setdefault('ip',{})
                    groupCollection[platform]['ip'].setdefault(value1,0)
                    groupCollection[platform]['ip'][value1] += 1
                if key1 == 'user_id':
                    if str(value1) == 'None' or str(value1) == '' or str(value1) == 'null' or str(value1) == '0':
                        continue                    
                    groupCollection[platform].setdefault('user_id',{})
                    groupCollection[platform]['user_id'].setdefault(value1,0)
                    groupCollection[platform]['user_id'][value1] += 1
                if str(platform) == 'mob':
                    if key1 == 'mac_addr':
                        if str(value1) == 'None' or str(value1) == '' or str(value1) == 'null' or str(value1) == '02:00:00:00:00:00' or str(value1) == '00:00:00:00:00:00':
                            continue                        
                        groupCollection[platform].setdefault('mac_addr',{})
                        groupCollection[platform]['mac_addr'].setdefault(value1,0)
                        groupCollection[platform]['mac_addr'][value1] += 1 
                    if key1 == 'idfv':
                        if str(value1) == 'None' or str(value1) == '' or str(value1) == 'null':
                            continue                        
                        groupCollection[platform].setdefault('idfv',{})
                        groupCollection[platform]['idfv'].setdefault(value1,0)
                        groupCollection[platform]['idfv'][value1] += 1 
                    if key1 == 'imei':
                        groupCollection[platform].setdefault('imei',{})
                        if str(value1) == 'None' or str(value1) == '' or str(value1) == 'null':
                            continue
                        groupCollection[platform]['imei'].setdefault(value1,0)
                        groupCollection[platform]['imei'][value1] += 1  
                        
        groupCollection[platform].setdefault(platform + 'totalcount',len(detail))
    return groupCollection
def calculate(key,groupCollection):
    global totalcount
    wordAllList = {}
    dataList ={}
    allcount = 0
    for platform,value in groupCollection.items():
        dataList.setdefault(str(platform),{})
        allcount += value[str(platform)+'totalcount']
        for key1,value1 in value.items():
            if 'totalcount' in key1:
                continue
            tsortedList = sorted(value1.items(),key=lambda x:x[1],reverse=True)
            try:
                top10List = tsortedList[0:10]
            except:
                top10List = tsortedList[0:] 
	    top10Str = ''
	    for item in top10List:
		top10Str += str(item[0]) + '-' + str(item[1])+'|'
	    top10Str = top10Str[:-1]
	    dataList[str(platform)].setdefault(key1,top10Str)

        dataList.setdefault(platform+'totalcount',value[str(platform)+'totalcount'])
    dataList.setdefault('totalcount',allcount)
    totalcount += allcount 
    databaseTuples = []
    databaseTuples.append(str(key))
    try:
        databaseTuples.append(len(groupCollection['mob']['ip']))
	databaseTuples.append(dataList['mob']['ip'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(len(groupCollection['mob']['user_id']))
	databaseTuples.append(dataList['mob']['user_id'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(len(groupCollection['mob']['idfv']))
	databaseTuples.append(dataList['mob']['idfv'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(len(groupCollection['mob']['mac_addr']))
	databaseTuples.append(dataList['mob']['mac_addr'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(len(groupCollection['mob']['imei']))
	databaseTuples.append(dataList['mob']['imei'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(round(float(len(groupCollection['mob']['ip']))/float(dataList['mobtotalcount'])*100,3))
        databaseTuples.append(round(float(dataList['mobtotalcount'])/float(dataList['totalcount'])*100,3))
    except:
        databaseTuples.extend([0.0,0.0])
    try:
        databaseTuples.append(len(groupCollection['pc']['ip']))
	databaseTuples.append(dataList['pc']['ip'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(len(groupCollection['pc']['user_id']))
	databaseTuples.append(dataList['pc']['user_id'])
    except:
	databaseTuples.extend([0,''])
    try:
        databaseTuples.append(round(float(len(groupCollection['pc']['ip']))/float(dataList['pctotalcount'])*100,3))
        databaseTuples.append(round(float(dataList['pctotalcount'])/float(dataList['totalcount'])*100,3))
    except:
        databaseTuples.extend([0.0,0.0])
        
        
    databaseTuples.append(dataList['totalcount'])
    lit1 = copy.deepcopy(databaseTuples)
    del databaseTuples
    return lit1
if __name__ == '__main__':
    logcut = logCut(dt)
    logcut.explode(0,100)
    dbwriter = logCutHelper.dbAccess()
    dbcolumns = ('tag_word','mob_ip_count','mob_top10_ip','mob_user_id_count','mob_top10_user_id','mob_idfv_count','mob_top10_idfv','mob_mac_addr_count','mob_top10_mac_addr','mob_imei_count','mob_top10_imei','mob_ip_group','mob_rate','pc_ip_count','pc_top10_ip','pc_user_id_count','pc_top10_user_id','pc_ip_group','pc_rate','totalcount','rate','createtime')
    resultList = []
    for num in range(0,1013):
	allList = pvsReduce(num)
	for obj_id,value in allList.items():
	    if str(obj_id) =='0' or str(obj_id) == '':
		continue
	    resultList.append(calculate(str(obj_id),pvsGroup(value)))
	print 'reduce:'+str(round(float(num)/1013,3)*100)+'%'
    alltotalCount = 0
    for item in resultList:
	alltotalCount += int(item[-1])
    for item in resultList:
	item.append(round(float(item[-1])/float(alltotalCount),3)*100)
	item.append(str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
	print dbwriter.writeT('t_pvs_refer_group',dbcolumns,item)
    for num in range(0,1013):
	try:
	    f = open(mapPath+str(num),'w')
	except:
	    pass
