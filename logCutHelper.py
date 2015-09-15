import sys
import MySQLdb
import time

class dbAccess(object):
    """databases helper of hadoopcube; author:zhiganggao"""
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
    def readT(self,dbName,column,where = 'id >= 0'):
        """read opreation
           dbName is the name of databases
           column is a list of select columns
           where is the where statement
           return result(list) when success
           return ['error in sql:',sql] when failed
        """
        sql = 'select '+','.join(column)+' from '+str(dbName)+' where '+str(where)
        try:
            self.__cursor.execute(sql)    
            results = self.__cursor.fetchall()	
            return results
        except:
            return ['error in sql:',sql]

    def writeT(self,dbName,column,value,muti = False):
        """write opreation 
           dbName is the name of databases
           column is a list of insert columns
           value is a list of insert values
           return ['success'] when success
           return ['error in sql:',sql] when failed
        """        
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
    
class strsCompare(object):
    def strCompare(self,str1,str2):
	"""compare two string return the same rate"""
	if reduce(lambda x,y:x < y,(len(str1),len(str2))):
	    str1,str2 = str2,str1
	str1List = []
	str2List = []
	if len(str1) > 10:
	    str1Len = 0
	    while str1Len + 10 < len(str1):
		str1List.append(str1[str1Len:str1Len + 10])
		str1Len += 10
	    str1List.append(str1[str1Len:])
	else:
	    str1List.append(str1)
	if len(str2) > 10:
	    str2Len = 0
	    while str2Len + 10 < len(str2):
		str2List.append(str1[str2Len:str2Len + 10])
		str2Len += 10
	    str2List.append(str2[str2Len:])
	else:
	    str2List.append(str2)
	resultList = []
	index = 0
	devodeNum = []
	for x in str1List:
	    try:
		resultList.append(self.__strCompareSplited(x, str2List[index]))
		devodeNum.append(float(len(x))/10)
	    except:
		resultList.append(0)
		devodeNum.append(1)
	    index += 1
	
	resultNum = 0
	count = len(resultList)
	for index in range(0,count):
	    resultNum += float(resultList[index])*(devodeNum[index]/reduce(lambda x,y:x+y,devodeNum))
	return resultNum
        
    def __strCompareSplited(self,str1,str2):
        """compare two string return the same rate"""
        if len(str1) == 0 and len(str2) == 0:
            return -1
        max_len = 0
        max_len_list = {}
        devia = abs(len(str1)-len(str2))+1
        if reduce(lambda x,y:x < y,(len(str1),len(str2))):
            str1,str2 = str2,str1
        is_set = False
        arrayIndex = -1
        for index in range(0,len(str2)):
            max_len_list.setdefault(index,[])
            for index1 in range(0,len(str1)):
                if not str2[index] == str1[index1]:
                    if index > 0 and index1 > 0:
                        if max_len_list[index - 1][index1 - 1] > max_len_list[index - 1][index1]:
                            
                            if max_len_list[index - 1][index1 - 1] > max_len_list[index][index1 - 1]:
                                max_past = max_len_list[index - 1][index1 - 1]
                            else:
                                max_past = max_len_list[index][index1 - 1]
                        else:
                            if max_len_list[index - 1][index1] > max_len_list[index][index1 - 1]:
                                max_past = max_len_list[index - 1][index1]
                            else:
                                max_past = max_len_list[index][index1 - 1]
                                
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
    def __maxStr(self,int1,int2,index,max_len_list,left = True,go = False): 
        if int1 >= len(max_len_list):
            return ''
        if int2 >= len(max_len_list[0]):
            return ''
        if max_len_list[int2][int1] == index:
            return str(self.__maxStr(int1, int2, index+1, max_len_list))+str(int1)+'|'
        elif left:
            return str(self.__maxStr(int1, int2+1, index, max_len_list,False,go))
        elif not left:
            if go:
                return str(self.__maxStr(int1+1, int2, index, max_len_list,True,False))
            else:
                return str(self.__maxStr(int1+1, int2-1, index, max_len_list,True,True))
        
class logCut(object):
    __platform = 0
    __mac_list = []
    __cata = ('shows','pvs')
    __path = "/data/scribelog/brd_ad/cpc/"
    __tplist = ('ip','user_id','idfv','imei','mac_addr')
    def __init__(self,platform,cata,dt):
        self.__platform = platform
	f2 = open('/home/brdwork/antispider/helper/mac_list','r')
	self.__mac_list = f2.readlines()
	f2.close()
        if platform == 1:
            self.__tplist = self.__tplist[0:2]
        dateList = time.localtime(dt)
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
        if cata == 1 or cata == 0:
            self.__path += "brdad_cpc_all_tid_shop_"+self.__cata[cata]+"/brdad_cpc_all_tid_shop_"+self.__cata[cata]+"-"+dateStr
        else:
            print 'wrong in cata'
        if (platform != 1) and (platform != 2):
            print 'wrong in planform'

    def splitWithLog(self,num,tidList = []):
        f = open(self.__path+str(num),'r')
        strs = list()
        tidStr = ''
        for item in tidList:
            tidStr += str(item)
	if tidStr == '':
            shows = []
        else:
            shows = {}
        dic ={}
        p = f.readlines()
        for unit in p:
            if len(unit) > 1:
                strs.extend(unit.split("\""))
        count = len(strs)
        if not tidStr == '' and self.__platform == 1:
            for index in range(count):
                if "\n" not in strs[index]:
                    if 'scribe_meta<new_logfile>' in strs[index]:
                        break;
                    if 'tid' == strs[index]:
                        dic['tid'] = strs[index+2]                                
                    if 'channel_sources' == strs[index]:
                        if len(strs[index+1]) == 1:dic['channel_source'] = strs[index+2]
                        else:dic['channel_source'] = strs[index+1][1:-1]
                    if 'ip' == strs[index]:
                        dic['ip'] = strs[index+1][1:-1]
                    if 'user_id' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
                        else:dic['user_id'] = strs[index+1][1:-1]
                    if 'user_agent' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
                        else:dic['user_agent'] = strs[index+1][1:-1] 
                    if 'unit_price' == strs[index]:
                        if len(strs[index+1]) == 1:dic['unit_price'] = strs[index+2] 
                        else:dic['unit_price'] = strs[index+1][1:-1]		
                    if 'uri' == strs[index]:
                        if len(strs[index+1]) == 1:dic['uri'] = strs[index+2] 
                        else:dic['uri'] = strs[index+1][1:-1] 
                    if 'time' == strs[index]:
                        dic['time'] = strs[index+1][1:-1]                
                else:
                    if not dic.get('tid',-1) == -1 and dic.get('tid', -1) in tidStr and int(dic.get('channel_source' ,-1)) >> 30 == 1:
                        shows.setdefault(dic.get('tid', -1),[])
                        shows[dic.get('tid', -1)].append(dic)
                    dic = {}
        if tidStr == '' and self.__platform == 1:
            for index in range(count):
                if "\n" not in strs[index]:
                    if 'scribe_meta<new_logfile>' in strs[index]:
                        break;
                    if 'tid' == strs[index]:
                        dic['tid'] = strs[index+2]                                
                    if 'channel_sources' == strs[index]:
                        if len(strs[index+1]) == 1:dic['channel_source'] = strs[index+2]
                        else:dic['channel_source'] = strs[index+1][1:-1]
                    if 'ip' == strs[index]:
                        dic['ip'] = strs[index+1][1:-1]
                    if 'user_id' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
                        else:dic['user_id'] = strs[index+1][1:-1]
                    if 'user_agent' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
                        else:dic['user_agent'] = strs[index+1][1:-1]  
                    if 'time' == strs[index]:
                        dic['time'] = strs[index+1][1:-1]                
                else:
                    if not dic.get('tid',-1) == -1:
                        shows.append(dic)
                    dic = {}
        if tidStr == '' and self.__platform == 2:
            for index in range(count):
                if "\n" not in strs[index]:
                    if 'scribe_meta<new_logfile>' in strs[index]:
                        break;
                    if 'tid' == strs[index]:
                        dic['tid'] = strs[index+2]                                
                    if 'channel_sources' == strs[index]:
                        if len(strs[index+1]) == 1:dic['channel_source'] = strs[index+2]
                        else:dic['channel_source'] = strs[index+1][1:-1]
                    if 'ip' == strs[index]:
                        dic['ip'] = strs[index+1][1:-1]
                    if 'user_id' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
                        else:dic['user_id'] = strs[index+1][1:-1]
                    if 'imei' == strs[index]:
                        if len(strs[index+1]) == 1:dic['imei'] = strs[index+2]
                        else:dic['imei'] = strs[index+1][1:-1]
                    if 'mac_addr' == strs[index]:
                        if len(strs[index+1]) == 1:dic['mac_addr'] = strs[index+2]
                        else:dic['mac_addr'] = strs[index+1][1:-1]
                    if 'idfv' == strs[index]:
                        if len(strs[index+1]) == 1:dic['idfv'] = strs[index+2]
                        else:dic['idfv'] = strs[index+1][1:5]		
                    if 'user_agent' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
                        else:dic['user_agent'] = strs[index+1][1:-1] 
                    if 'time' == strs[index]:
                        dic['time'] = strs[index+1][1:-1]                
                else:
                    if not dic.get('tid',-1) == -1 and int(dic.get('channel_source' ,-1)) >> 30 == 2:
                        shows.append(dic)
                    dic = {}
        if not tidStr == '' and self.__platform == 2:
            for index in range(count):
                if "\n" not in strs[index]:
                    if 'scribe_meta<new_logfile>' in strs[index]:
                        break;
                    if 'tid' == strs[index]:
                        dic['tid'] = strs[index+2]                                
                    if 'channel_sources' == strs[index]:
                        if len(strs[index+1]) == 1:dic['channel_source'] = strs[index+2]
                        else:dic['channel_source'] = strs[index+1][1:-1]
                    if 'ip' == strs[index]:
                        dic['ip'] = strs[index+1][1:-1]
                    if 'user_id' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_id'] = strs[index+2]
                        else:dic['user_id'] = strs[index+1][1:-1]
                    if 'imei' == strs[index]:
                        if len(strs[index+1]) == 1:dic['imei'] = strs[index+2]
                        else:dic['imei'] = strs[index+1][1:-1]
                    if 'mac_addr' == strs[index]:
                        if len(strs[index+1]) == 1:dic['mac_addr'] = strs[index+2]
                        else:dic['mac_addr'] = strs[index+1][1:-1]
                    if 'user_agent' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
                        else:dic['user_agent'] = strs[index+1][1:-1] 
                    if 'unit_price' == strs[index]:
                        if len(strs[index+1]) == 1:dic['unit_price'] = strs[index+2] 
                        else:dic['unit_price'] = strs[index+1][1:-1]                    
                    if 'idfv' == strs[index]:
                        if len(strs[index+1]) == 1:dic['idfv'] = strs[index+2]
                        else:dic['idfv'] = strs[index+1][1:5]		
                    if 'user_agent' == strs[index]:
                        if len(strs[index+1]) == 1:dic['user_agent'] = strs[index+2] 
                        else:dic['user_agent'] = strs[index+1][1:-1] 
                    if 'uri' == strs[index]:
                        if len(strs[index+1]) == 1:dic['uri'] = strs[index+2] 
                        else:dic['uri'] = strs[index+1][1:-1]                    
                    if 'time' == strs[index]:
                        dic['time'] = strs[index+1][1:-1]                
                else:
                    if not dic.get('tid',-1) == -1 and dic.get('tid', -1) in tidStr and int(dic.get('channel_source' ,-1)) >> 30 == 2:
                        shows.setdefault(dic.get('tid', -1),[])
                        shows[dic.get('tid', -1)].append(dic)
                    dic = {}        
        f.close()
        return shows
    def summry(self,shows,record = {}):
        
        if self.__platform == 2:
            for v2 in shows:	
                try:
                    dateConvert = time.localtime(int(v2['time']))
                except:
                    return
                record.setdefault(v2['tid'],{})
                record[v2['tid']].setdefault('tid',v2['tid'])
                for tp in self.__tplist:
                    record[v2['tid']].setdefault(tp+"count",0)
                    record[v2['tid']].setdefault(tp+"collection",{})
                    record[v2['tid']][tp+"collection"].setdefault(v2[tp],0)
                record[v2['tid']].setdefault('count',0)
                record[v2['tid']].setdefault('ilcount',0)
		record[v2['tid']].setdefault('ilmac',0)
                record[v2['tid']].setdefault('timecollection',{})	
                record[v2['tid']].setdefault('illegalcount',0)
                for index in range(0,24):
                    record[v2['tid']]['timecollection'].setdefault(index,0)
                try:
                    #record[v2['tid']]['timecollection'].setdefault(dateConvert[3],0)
            
                    record[v2['tid']]['count'] += 1
                    record[v2['tid']]['timecollection'][dateConvert[3]] += 1
                except ValueError:
                    pass
                try:
                    if not int(v2['ip']) == 2130706433:
                        record[v2['tid']]['ipcount'] += 1
                        record[v2['tid']]['ipcollection'][v2['ip']] += 1
                except:
                    pass
                try:
                    if not (str(v2['mac_addr']) == '02:00:00:00:00:00' or str(v2['mac_addr']) == 'null' or str(v2['mac_addr'])=='00:00:00:00:00:00'):
			if not str(v2['mac_addr']).replace(':','')[0:6]+'\n' in self.__mac_list:
	                    record[v2['tid']]['ilmac'] += 1
                        record[v2['tid']]['mac_addrcount'] += 1
                        record[v2['tid']]['mac_addrcollection'][v2['mac_addr']] += 1
                except:
                    pass
                try:
                    if not int(v2['imei']) == 'null':
                        record[v2['tid']]['imeicount'] += 1
                        record[v2['tid']]['imeicollection'][v2['imei']] += 1
                except:
                    pass
                try:
                    if not str(v2['idfv']) == 'null':
                        record[v2['tid']]['idfvcount'] += 1
                        record[v2['tid']]['idfvcollection'][v2['idfv']] += 1	
                except:
                    pass
                try:
                    if re.match(r'.*(php|java|javascript|perl|python|bingbot|Soso|Yahoo|Sogou.*spider|Googlebot|Baiduspider|Baiduspider|EasouSpider|WinHttp).*',v2['user_agent'],re.M|re.I):
                        record[v2['tid']]['illegalcount'] += 1
                    if re.match(r'.*[g-z].*',v2['mac_addr'],re.M|re.I):
                        record[v2['tid']]['illegalcount'] += 1		
                except:
                    pass 
                if not str(v2['user_id']) == '0':
                    record[v2['tid']]['user_idcount'] += 1
                    record[v2['tid']]['user_idcollection'][v2['user_id']] += 1
        else:
            for v2 in shows:
                if int(v2['channel_source'])>>30 == 1:
                    pass
                else:
            
                    record.setdefault(v2['tid'],{})
                    record[v2['tid']].setdefault('totalcount',0)
                    record[v2['tid']]['totalcount'] += 1
                    continue	
                try:
                    dateConvert = time.localtime(int(v2['time']))
                except:
                    return
                record.setdefault(v2['tid'],{})
                record[v2['tid']].setdefault('tid',v2['tid'])
                for tp in self.__tplist:
                    record[v2['tid']].setdefault(tp+"count",0)
                    record[v2['tid']].setdefault(tp+"collection",{})
                    record[v2['tid']][tp+"collection"].setdefault(v2[tp],0)
                record[v2['tid']].setdefault('count',0)
                record[v2['tid']].setdefault('totalcount',0)
                record[v2['tid']]['totalcount'] += 1
                record[v2['tid']].setdefault('pccount',0)
                record[v2['tid']].setdefault('ilcount',0)
                record[v2['tid']].setdefault('timecollection',{})	
                record[v2['tid']].setdefault('illegalcount',0)
                record[v2['tid']]['pccount'] += 1
                for index in range(0,24):
                    record[v2['tid']]['timecollection'].setdefault(index,0)
                try:
                    #record[v2['tid']]['timecollection'].setdefault(dateConvert[3],0)
            
                    record[v2['tid']]['count'] += 1
                    record[v2['tid']]['timecollection'][dateConvert[3]] += 1
                except ValueError:
                    pass
                try:
                    if not int(v2['ip']) == 2130706433:
                        record[v2['tid']]['ipcount'] += 1
                        record[v2['tid']]['ipcollection'][v2['ip']] += 1
                except:
                    pass
                try:
                    if re.match(r'.*(php|java|javascript|perl|python|bingbot|Soso|Yahoo!|Sogou*.spider|Googlebot|Baiduspider|Baiduspider|EasouSpider).*',v2['user_agent'],re.M|re.I):
                        record[v2['tid']]['illegalcount'] += 1
                    if re.match(r'.*[g-z].*',v2['mac_addr'],re.M|re.I):
                        record[v2['tid']]['illegalcount'] += 1		
                except:
                    pass 
                if not str(v2['user_id']) == '0':

                    record[v2['tid']]['user_idcount'] += 1
                    record[v2['tid']]['user_idcollection'][v2['user_id']] += 1
        return record
if __name__ == '__main__':
    try:
        db_opreater = dbAccess()
        if len(db_opreater.readT('t_pvs_log_analysis_mob', ['distinct createtime '], "createtime = '2015-08-03'")) >1:
            print 'query error'
        else:
            print 'query normal'
    except:
        print 'query init failed'
    try:
        strCompare = strsCompare()
        if strCompare.strCompare('1234', '12345') == 0.8:
            print 'strCompare normal'
        else:
            print 'strCompare error'
    except:
        print 'strCompare init failed'
    try:
        logcut = logCut(1, 1, time.time()-86400)
        if len(logcut.summry(logcut.splitWithLog('0000'))) > 1:
            print 'logcut normal'
        else:
            print 'logcut error'
    except:
        print 'logcut init failed'
            
        
