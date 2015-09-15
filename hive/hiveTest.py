from hivehelper import hivehelper
import json

hp = hivehelper()
f = open('list1','r')
list1 = f.readlines()
list2 = []
for x in list1:
    m = str(x).split('|')
    list2.append(m)
for twi in list2:
    sql = "select user_id,client_ip,unit_price,refer,agent,log_date,hour,minute,second,twitter_id from ad_cpc_pvs_log_pc where dt = '"+str(twi[0])+"' and twitter_id  = "+str(twi[1])
    sql1 = "select user_id,client_ip,unit_price,refer,agent,log_date,hour,minute,second,twitter_id from ad_cpc_pvs_log_mob where dt = '"+str(twi[0])+"' and twitter_id  = "+str(twi[1])
    print sql
    print sql1
    allsummry = hp.hive_execute_all(sql)
    mobsummry = hp.hive_execute_all(sql1)
    allsummry.extend(mobsummry)
    if len(allsummry) == 0:
	continue 
    f = open("antilist/"+str(twi[0])+'-'+str(twi[1])[:-1]+'.csv','w+')
    for item in allsummry:
#	j1 = json.dumps(item)
#	j2 = json.loads(j1)
	#print type(j2)
        f.write(json.dumps(item) + "\n")
    f.flush()
    f.close() 
