from hivehelper import hivehelper

hp = hivehelper()
sql = "select user_id,client_ip,unit_price,refer,agent,log_date,hour,minute,second,twitter_id from ad_cpc_pvs_log_pc where dt = '2015-07-16' and twitter_id  = 3668029553"
sql1 = "select user_id,client_ip,unit_price,refer,agent,log_date,hour,minute,second,twitter_id from ad_cpc_pvs_log_mob where dt = '2015-07-16' and twitter_id  = 3668029553"
allsummry = hp.hive_execute_all(sql)
mobsummry = hp.hive_execute_all(sql1)
allsummry = allsummry.extend(mobsummry)
f = open('3668029553.csv','w+')
for item in allsummry:
    f.write(str(item).replace('[','').replace(']',''))
f.flush()
f.close()