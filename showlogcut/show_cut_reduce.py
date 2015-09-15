import sys 
import time
import json
reload(sys)
sys.setdefaultencoding('utf-8')

if not "/home/brdwork/antispider/helper/" in sys.path:
    sys.path.append("/home/brdwork/antispider/helper/")
if not 'logCutHelper' in sys.modules:
    logCutHelper = __import__('logCutHelper')
else:
    eval('import logCutHelper')
    logCutHelper = eval('reload(logCutHelper)')

date = time.time()-86400
dateList = time.localtime(date)
path = "/home/brdwork/antispider/data/showsmap/range-"
def excute(path,range_x):
    excuteList = []
    for index in range(range_x[0],range_x[1]):
        if index < 10:
            fpath = path + "000"+str(index)
        elif index < 100:
            fpath = path + "00"+str(index)
        elif index < 1000:
            fpath = path + "0"+str(index)
        else:
            fpath = path + str(index)
        excuteList.append(group(splitWithTemp(fpath)))
        print fpath
    return excuteList
def splitWithTemp(path):
    f = open(path,'r')
    lines = f.readlines()
    returnList = []
    f.close()
    for line in lines:
        try:
            dic = json.loads(line[:-1])
        except:
            dic = {}
        returnList.append(dic)
    return returnList
def group(allList):
    groupCollection = {}
    for value in allList:
        for key1,value1 in value.items():
	    if key1 == '0':
		continue
	    groupCollection.setdefault(key1,{})
	    for key2,value2 in value1.items():
		groupCollection[key1].setdefault(str(key2),{})
		for key3,value3 in value2.items():
		    if key2 == 'channel_sources':
			key3 = str(int(key3) >> 30)
		    if key2 == 'user_id' and str(key3) == '0':
			continue
		    groupCollection[key1][str(key2)].setdefault(str(key3),0)
		    groupCollection[key1][str(key2)].setdefault(str(key2)+'totalcount',0)
		    groupCollection[key1][str(key2)][str(key3)] += int(value3)
		    groupCollection[key1][str(key2)][str(key2)+'totalcount'] += int(value3)
    return groupCollection
def calculate(groupCollection):
    global dateList
    dbwriter = logCutHelper.dbAccess()
    totalcount = 0
    wordAllList = {}
    for y in groupCollection:
	for key,value in y.items():
	    wordAllList.setdefault(key,{})
	    for key1,value1 in value.items():
		tsortedList = sorted(y[key][key1].items(),key=lambda x:x[1],reverse=True)
		try:
		    top10List = tsortedList[1:11]
		except:
		    top10List = tsortedList[1:]
		wordAllList[key].setdefault(key1,top10List)
		wordAllList[key].setdefault(key1+'count',len(y[key][key1])-1)
	    wordAllList[key].setdefault('totalcount',y[key]['ip']['iptotalcount'])
	    totalcount += y[key]['ip']['iptotalcount']
    databaseColumns = ('tag_word','ip_count','user_id_count','ip_group','user_id_group','pc_rate','mob_rate','rate','totalcount','mob_count','top10_ip','top10_user_id','createtime')
    for key,value in wordAllList.items():
	databaseTuples = []
	databaseTuples.append(str(key))
	databaseTuples.append(value['ipcount'])
	databaseTuples.append(value['user_idcount'])
	databaseTuples.append(round(float(value['ipcount'])/float(value['totalcount'])*100,3))
	databaseTuples.append(round(float(value['user_idcount'])/float(value['totalcount'])*100,3))
	pcandmobrate = {}
	pcandmobrate.setdefault('pc',0)
	pcandmobrate.setdefault('mob',0)
	for everytuple in value['channel_sources']:
	    #print everytuple
	    if everytuple[0] == '2':
		pcandmobrate['mob'] = everytuple[1]
	    else:
		pcandmobrate['pc'] = everytuple[1]
	databaseTuples.append(round(float(pcandmobrate['pc'])/float(value['totalcount'])*100,3))
	databaseTuples.append(round(float(pcandmobrate['mob'])/float(value['totalcount'])*100,3))
	databaseTuples.append(round(float(value['totalcount'])/float(totalcount)*100,3))
	databaseTuples.append(float(value['totalcount']))
	databaseTuples.append(int(pcandmobrate['mob']))
	ipStr = ''
	for everytuple in value['ip']:
	    if not 'totalcount' in everytuple[0]:
		ipStr += str(everytuple[0])+':'+str(everytuple[1])+'|'
	ipStr = ipStr[:-1]
	databaseTuples.append(ipStr)
	user_idStr = ''
	for everytuple in value['user_id']:
	    user_idStr += str(everytuple[0])+':'+str(everytuple[1])+'|'
	user_idStr = user_idStr[:-1]	
	databaseTuples.append(user_idStr)
	databaseTuples.append(str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]))
	print dbwriter.writeT('t_shows_refer_group',databaseColumns,databaseTuples)
    
if __name__ == '__main__':
    calculate(excute(path,(0,1013)))
    for index in range(0,1013):
        if index < 10:
            fpath = path + "000"+str(index)
        elif index < 100:
            fpath = path + "00"+str(index)
        elif index < 1000:
            fpath = path + "0"+str(index)
        else:
            fpath = path + str(index)
	f = open(fpath,'w')        
