import sys
import MySQLdb
import time
import re

if not "/home/brdwork/antispider/helper/" in sys.path:
    sys.path.append("/home/brdwork/antispider/helper/")
if not 'logCutHelper' in sys.modules:
    logCutHelper = __import__('logCutHelper')
else:
    eval('import logCutHelper')
    logCutHelper = eval('reload(logCutHelper)')

xList = {}
setList = ('user_agent','ip','user_id','imei','idfv','mac_addr')
date = time.time()-86400
dateList = time.localtime(date)
dateStr = str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2])
writePath = "/home/brdwork/antispider/data/detail/detail_mob-"+dateStr
record = {}
def readList():
    global xList
    dbreader = logCutHelper.dbAccess()
    tidList = []
    yList = dbreader.readT('t_malicious_report_mob', ['twitter_id','pvs','ip_group','user_group','imei_group','idfv_group','mac_group'], "pvs > 300 and degree > 8 and createtime = '"+dateStr+"'")
    for v in yList:
        xList.setdefault(v[0],{})
        xList[v[0]]['count'] = v[1]
        xList[v[0]]['group1'] = v[2]
        xList[v[0]]['group2'] = v[3]
        xList[v[0]]['group3'] = v[4]
        xList[v[0]]['group4'] = v[5]
	xList[v[0]]['group5'] = v[6]
        tidList.append(v[0])
    return tidList

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
    str_compare = logCutHelper.strsCompare()
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
			search_key_list1 = value.split(":")
                        for search_key in search_key_list1:
                            if 'tag_word' in search_key:
                                searchKey = search_key[9:]
                        keys = recordCollection[k]['refer'].keys()
                        for x in keys:
			    if float(len(x))/float(len(searchKey))>0.8:
				if str_compare.strCompare(x,searchKey) > 0.85:
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
    db_writer = logCutHelper.dbAccess()
    f2 = open('/home/brdwork/antispider/helper/mac_list','r')
    mac_list = f2.readlines()
    f2.close()    
    for k,v in recordCollection.items():
        affectList.setdefault('all' ,[])
        affectList.setdefault('cpc' ,[])
        affectList.setdefault('refererror' ,[])
        affectList.setdefault('refernojuage' ,[])
        affectList.setdefault('referaffect' ,[])
        affectList.setdefault('single' ,[])	
        k = int(k)
        agentGroup = float(len(v['user_agent'])/xList[k]['count'])
	reasonList = {}
	reasonList.setdefault('referMax',0)
	reasonList.setdefault('groupError',0)
	reasonList.setdefault('ilRefer',0)
	
	reasonList.setdefault('ilMac',0)
	reasonList.setdefault('max',0)
        for count_refer in v['refer'].values():
            if float(len(count_refer))/xList[k]['count'] > 0.85:
		reasonList['referMax'] += 1
                affectList['all'].extend(count_refer)
        for i in range(1,5):
            if float(xList[k]['group'+str(i)]) < 0.5 and (abs(float(xList[k]['group'+str(i)]) - agentGroup) < 0.1 or agentGroup < 0.2):
                for m in v[setList[i]].values():
                    if len(m) > int(1.0/float(float(xList[k]['group'+str(i)])+0.01)):
                        for n in m:
                            if affectList['all'].count(n[0]) < 1:
				reasonList['groupError'] += 1
                                affectList['all'].append(n[0])
        indexil = 0
        for ilrefer in v['ilreferoragent']:
            if ilrefer[0] == 1:
		if affectList['all'].count(indexil) < 1:
		    reasonList['ilRefer'] += 1
		    affectList['all'].append(indexil)
                #print ilrefer[1]
            indexil += 1
#####################
	macCollection = {'il':[],'af':[]}
	for mac_addr,macSet in v['mac_addr'].items():
	    if str(mac_addr) == '02:00:00:00:00:00' or str(mac_addr) == '00:00:00:00:00:00':
		continue
	    if str(mac_addr).replace(':','')[0:6]+'\n' in mac_list:
		macCollection['af'].extend(macSet)
	    else:
		macCollection['il'].extend(macSet)
	if len(macCollection['il'])/(len(macCollection['af'])+1) > 1.5:
	    for mac_addr in macCollection['il']:
		if affectList['all'].count(mac_addr[0]) < 1:
		    reasonList['ilMac'] += 1
		    affectList['all'].append(mac_addr[0])			
	maxset = {}
        for xset in setList:
            maxset.setdefault(xset,(0,''))
            for key,value in v[xset].items():
                if str(key) == 'null' or str(key) == '0' or  str(key) == '02:00:00:00:00:00' or  str(key) == '00:00:00:00:00:00' or str(key) == '2130706433':
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
			reasonList['max'] += 1
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
	reasonStr = ''
	for key,value in reasonList.items():
	    reasonStr += str(key) + str(value) + "|"
	reasonStr = reasonStr[:-1]
        column = ('twitter_id','pvs','ip_group','user_group','er_refer_count','single_count','af_refer_count','possible_malicious_count','af_count','af_cpc_count','createtime','reason')
        value = (int(k),int(xList[k]['count']),float(xList[k]['group1']),float(xList[k]['group2']),int(len(affectList['refererror'])),int(len(affectList['single'])),\
                 int(len(affectList['refernojuage'])),int(len(affectList['all'])),int(len(affectList['referaffect'])),int(len(affectList['cpc'])),str(dateList[0])+'-'+str(dateList[1])+'-'+str(dateList[2]),str(reasonStr))
        print db_writer.writeT('t_malicious_result_mob_test', column, value)
        affectList = {}                                                                                                                                                


####################
def splitLog(tidList):
    global date
    excuteRecord = list() 
    logcut = logCutHelper.logCut(2, 1, date)
    for index in range(200):
        if len(str(index)) == 1:
            num = "000"+str(index)
        else:
            if len(str(index)) == 2:
                num = "00"+str(index)
            else:num = '0'+str(index)
        try:   
            for k,v in logcut.splitWithLog(num,tidList).items():
                record.setdefault(k, [])
                record[k].extend(v)                        
            excuteRecord.append(dateStr+str(index)+"completed\n")
            print(dateStr+str(index)+"completed")
        except IOError:
            continue
    return excuteRecord

if __name__ == '__main__':
    splitLog( readList())
    writeDetail()
    calculate(group())
