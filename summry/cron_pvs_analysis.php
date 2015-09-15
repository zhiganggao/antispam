<?php 
/**
 * @author zhiganggao
 */
//ini_set("display_errors",'On');
date_default_timezone_set('Asia/Shanghai');
require_once '/home/brdwork/data/release/brdpro8/brdlib/stdafx.php';//brdlib入口类

$dateToCheck = strtotime(date('Y-m-d',time()-2*86400));//查询时间定义，默认昨天

class dbAccess
{
	private $dsn = 'mysql:host=172.16.0.188;dbname=test;port=3316';
	private $username = 'mlsreader';
	private $password = 'RMlSxs&^c6OpIAQ1';
	private static $db;
	private $platform = array('_pc','_mob','');
	private $report_db;
	private function __construct()
	{
		$this->report_db = new PDO($this->dsn,$this->username,$this->password);
		$this->report_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
	}
	public static function getDb()
	{
		if(empty(self::$db))
			self::$db = new dbAccess();
		return self::$db;
	}
	public function readPvs($platform,$colum,$where)
	{
		$columString = implode(",", $colum);
		$sql = 'select '.$columString." from t_pvs_log_analysis".$this->platform[$platform].' where '.$where;
		$sth = $this->report_db->prepare($sql);
		$sth->execute();
		$data = array();
		while($row =$sth->fetch(PDO::FETCH_ASSOC)){
			$data[] = $row;
		}
		return $data;
	}
	public function readShows($platform,$colum,$where)
	{
		$columString = implode(",", $colum);
		$sql = 'select '.$columString." from t_shows_log_analysis".$this->platform[$platform].'_test where '.$where;
		$sth = $this->report_db->prepare($sql);
		$sth->execute();
		$data = array();
		while($row =$sth->fetch(PDO::FETCH_ASSOC)){
			$data[] = $row;
		}
		return $data;
	}
	public function write($platform,$colum,$value)
	{
		$columString = implode(",", $colum);
		$valueString = '';
		foreach($value as $item){
			if(is_string($item))
			{
				$valueString .= "'".$item."',";
				
			}
			else{
				$valueString .= $item.",";
			}
		}
		$valueString = substr($valueString,0,-1);
		$sql = 'insert into t_malicious_report'.$this->platform[$platform].' ('.$columString.") values (".$valueString.")";
		try{
			$sth = $this->report_db->prepare($sql);
			$sth->execute();
		}
		catch (Exception $e)
		{
			return  $e.$sql;
		}
		return 'suc';
	}
}

/**
 * 异常订单twi列表提取
 * @param 查询时间 $dateToCheck
 * @return 异常订单twi列表提取
 */
function orderPick($dateToCheck)
{
	$where = 'ctime > ? and ctime < ? and price > 5 group by twitter_id order by ids desc';
	$where1 = 'ctime > ? and ctime < ? and price > 5 and status = 0 group by twitter_id order by ids desc';
	$value = array($dateToCheck, $dateToCheck+86400);
	$date = date('Ym',$dateToCheck);
	$sql1 = array(
			'where'=>array(
					'str'=>$where,
					'value'=>$value
			),
			'colum'=>array('count(id) ids','twitter_id'),
			'tab'=>$date
	);
	$sql2 = array(
			'where'=>array(
					'str'=>$where1,
					'value'=>$value
			),
			'colum'=>array('count(id) ids','twitter_id'),
			'tab'=>$date
	);
	$detailOrderCreate = svcExec('statistics','read_shop_order_data_sql_get',array($sql1,'twitter_id'));
	$detailOrderNotPay = svcExec('statistics','read_shop_order_data_sql_get',array($sql2,'twitter_id'));
	$compare = array();
	foreach ($detailOrderCreate as $k=>$v)
	{
		$compare[$k] = $detailOrderNotPay[$k]['ids']/$v['ids']*$detailOrderNotPay[$k]['ids'];
	}
	arsort($compare);
	$count = 0;
	foreach ($compare as $k=>$v)
	{
		if($v<50)
			break;
		$order[$k] = array('twitter_id'=>$k,'createOrder'=>$detailOrderCreate[$k]['ids'],'notPayOrder'=>$detailOrderNotPay[$k]['ids']
				,'rate'=>$v);
		$count++;
	}
	return $order;
}
/**
 *高点击twi列表提取
 * @param 查询时间 $dateToCheck
 * @return 高点击twi列表提取
 */
function pvsSummry($dateToCheck)
{
	$date = date('Y-m-d',$dateToCheck);
	$where = "createtime = '".$date."' order by pvs desc";
	$dbreader = dbAccess::getDb();
	$data = $dbreader->readPvs(0, array('pvs','twitter_id'), $where);
	$log = array();
	foreach ($data as $v)
	{
		$log[$v['twitter_id']]=$v['pvs'];
	}
	return $log;
}
/**
 *筛选出来的twi信息补全
 * @param 查询时间 $dateToCheck
 * @return 筛选出来的twi信息补全
 */
function allSummry($dateToCheck)
{
	$notInOrderList = array();
	$count = 0;
	$order = orderPick($dateToCheck);
	$log = pvsSummry($dateToCheck);
	foreach ($log as $k=>$v)
	{
		if($v<50)
			break;
		$count++;
		if(!empty($order[$k]))
		{
			$outdata[] = array('twitter_id'=>$k,'pvs'=>$v,'createOrder'=>$order[$k]['createOrder'],'notPayOrder'=>$order[$k]['notPayOrder'],
					'rate'=>$order[$k]['createOrder']);
		}
		else
		{
			$where = 'ctime > ? and ctime < ? and price > 5 and twitter_id = ? order by ids desc';
			$where1 = 'ctime > ? and ctime < ? and price > 5 and status = 0 and twitter_id = ? order by ids desc';
			$value = array($dateToCheck , $dateToCheck+ 86400,$k);
			$detailOrderCreate = svcExec('statistics','read_shop_order_data_sql_get',array(array(
					'where'=>array(
							'str'=>$where,
							'value'=>$value
					),
					'colum'=>array('count(id) ids'),
					'tab'=>date("Ym",$dateToCheck)
			)));
			$detailNotPay = svcExec('statistics','read_shop_order_data_sql_get',array(array(
					'where'=>array(
							'str'=>$where1,
							'value'=>$value
					),
					'colum'=>array('count(id) ids'),
					'tab'=>date("Ym",$dateToCheck)
			)));
			$outdata[] = array('twitter_id'=>$k,'pvs'=>$v,'createOrder'=>$detailOrderCreate[0]['ids'],'notPayOrder'=>$detailNotPay[0]['ids'],
					'rate'=>$detailNotPay[0]['ids']/$detailOrderCreate[0]['ids']*$detailNotPay[0]['ids']);
		}
	}
	return $outdata;
}
/**
 *	结果写入文件
 * @param csv表头 $showHeader；csv数据 $outData
 * @return 无
 */
function writeFile($showHeader,$outData)
{
	$file_path = '/home/brdwork/data/download/';
	$filename = $file_path . md5(date('YmdH')) . '_malicious_analysis.csv';
	echo $filename."\n";
	if (file_exists($filename)) {
		unlink($filename);
	}
	$handle = fopen($filename, 'w');
	$header_arr = array_map(function ($val) { return $val['show']; }, $showHeader);
	$heaer_str = '"' . implode('","', $header_arr) . '"';
	$heaer_str = mb_convert_encoding($heaer_str, "GBK", "UTF-8");
	fwrite($handle, $heaer_str . "\r\n");
	foreach ($outData as $key => $list) {
		$row = array();
		foreach ($showHeader as $k => $un) {
			$row[$key][$k] = $list[$k];
		}
		$row_str = '"' . implode('","', $row[$key]) . '"';
		$row_str = mb_convert_encoding($row_str, "GBK", "UTF-8");
		fwrite($handle, $row_str . "\r\n");
	}
	fclose($handle);
}

/**
 * 对符合条件的数据作进一步筛选，筛选出高风险列表
 * @param 查询日期 $dateToCheck
 * @return 筛选结果
 */
function screening($dateToCheck)
{
	$screen = array();
	$summry = allSummry($dateToCheck);
	$count = count($summry);
	for($i = 0;$i<$count;$i++)
	{
		$item = array_shift($summry);
		if($item['createOrder']-$item['notPayOrder'] < 50)
		{
			$screen[$item['twitter_id']] = $item;
		}
		if($item['rate']>35)
		{
			$screen[$item['twitter_id']] = $item;
		}
		if($item['createOrder']/$item['pvs']>0.3)
		{
			$screen[$item['twitter_id']] = $item;
		}
	}
	return $screen;
}
/**
 * 对高风险列表数据进行判断，确定其是否真的有风险嫌疑，并获取其嫌疑，生成每天的嫌疑列表
 * @param unknown $dateToCheck
 */
function juage($dateToCheck)
{
	$juageList = array();
	//$checkarray = array('ip','imei','mac','idfv','user');
	$checkarray = array('ip','user');
	$dbreader = dbAccess::getDb();
	$result = allSummry($dateToCheck);	
	foreach ($result as $k=>$v2)
	{
		$date = date('Y-m-d',$dateToCheck);
		$selectColumn = array('ip_total_count','ip_count','ip_max','user_total_count','user_count','user_max','pc_rate','pc_count','time_deviation','illegal_count');
		$where = "createtime = '".$date."' and twitter_id = ".$v2['twitter_id'];
		$data = $dbreader->readPvs(0, $selectColumn, $where);
		$row = $data[0];
		$item = array();
		$degree = 0;
		foreach ($checkarray as $v)
		{
			$group = $row[$v.'_count']/$row[$v.'_total_count'];
			$maxIndex = explode("|",$row[$v.'_max']);
			$item[$v.'_group'] = $group;
			$item[$v.'_max'] = $row[$v.'_max'];
			//print $v.":    ".$group."    ".$maxIndex[1]."\n";
			if(!empty($group) && !empty($maxIndex[1]))
				$degree += log(pow(2,(1-$group)*(1-$group)))*29;
			if($maxIndex[1] >= 20)
				$degree += 20/pow(2,100/$maxIndex[1]);
		}
		$item['pc_rate'] = $row['pc_rate'];
		if(!empty($row['pc_rate']))
			$degree += log(pow(2,(($row['pc_rate']/100)*($row['pc_rate']/100))))*29;
		if($row['time_deviation'] > 0.8)
			$degree += 40/pow(2,pow(2,9/(4*$row['time_deviation']*$row['time_deviation'])));
		$item['twitter_id'] = $v2['twitter_id'];
		$item['time_deviation'] = $row['time_deviation'];
		$item['pvs'] = $v2['pvs'];
		$item['degree'] = $degree;
		$juageList[] = $item;
		
	}
	$showHeader = array(
		'twitter_id'=>array('show'=>'推ID','class'=>'c50'),
			'pvs'=>array('show'=>'点击','class'=>'c50'),
			'ip_group'=>array('show'=>'ip集群','class'=>'c50'),
			'ip_max'=>array('show'=>'ip最大访问','class'=>'c50'),
			'user_group'=>array('show'=>'user_id集群','class'=>'c50'),
			'user_max'=>array('show'=>'user_id最大访问','class'=>'c50'),
			'time_deviation'=>array('show'=>'时间偏差','class'=>'c50'),
			'pc_rate'=>array('show'=>'pc端比例','class'=>'c50'),
			'degree'=>array('show'=>'分数','class'=>'c50'),
	);
	data_write($juageList);
	//writeFile($showHeader, $juageList);
	
}
/**
 * 两天内gmv变化与user_id集群统计
 * @param 宝贝ID $twitter_id，日期 $date
 * @return $list
 */
function order_detail($twitter_id,$dateToCheck)
{
	$where = 'ctime > ? and ctime < ? and twitter_id = ? ';
	$value = array($dateToCheck,$dateToCheck+86400,$twitter_id);
	$value1 = array($dateToCheck-86400,$dateToCheck,$twitter_id);
	$date = date('Ym',$dateToCheck);
	$date1 = date('Ym',$dateToCheck-86400);
	$sql = array(
			'where'=>array(
					'str'=>$where,
					'value'=>$value
			),
			'colum'=>array('order_id','income'),
			'tab'=>$date
	);
	$sql2 = array(
			'where'=>array(
					'str'=>$where,
					'value'=>$value1
			),
			'colum'=>array('income'),
			'tab'=>$date1
	);
	$orderList = svcExec('statistics','read_shop_order_data_sql_get',array($sql));
	$orderList1 = svcExec('statistics','read_shop_order_data_sql_get',array($sql2));
 	$orderListDetail = array ();
 	$user_id_group = array();
 	$total = 0;
 	$gmvN = 0;
 	$gmvP = 0; 
    foreach ( $orderList as $oneData ) { 
        $orderListDetail [] = $oneData ['order_id'];
        $gmvN += $oneData['income'];
    }
    foreach ( $orderList1 as $oneData ) {
    	$gmvP += $oneData['income'];
    }  
	$orderListStr = "(".implode(',', $orderListDetail).")";
	$sql1 = array(
			'where'=>array(
					'str'=>"order_id in {$orderListStr}",
			),
			'colum'=>array('buyer_uid'),
	);
	$detailList = svcExec('shop','read_shop_order_sql_get',array($sql1,''));
	foreach ( $detailList as $oneData ) {
		if(empty($user_id_group[$oneData['buyer_uid']]))
			$user_id_group[$oneData['buyer_uid']] = 1;
		else 
			$user_id_group[$oneData['buyer_uid']]++;
		$total++;
	}
	if($total > 50)
		return array($gmvP,$gmvN,count($user_id_group)/$total,max($user_id_group)/$total,array_search(max($user_id_group), $user_id_group),max($user_id_group),$total);
	else 
		return array($gmvP,$gmvN,100,0,0,0,$total);
}
/**
 * 计费变化统计
 * @param 宝贝id $twitter_id
 * @param 日期 $date
 * @return 差值
 */
function amount_de($twitter_id,$date)
{
	$dateP = date('Y-m-d',strtotime($date)-86400);
	$sqlStatistic = array(
			'where' => array('str' => "date in ('{$date}','{$dateP}') and twitter_id = ? group by date",
					'value'=>array($twitter_id)
			),
			'colum' => array('date','sum(amount) as sum'
			),
			'stime' =>$date,
			'etime' =>$date
	);
	$detailList = svcExec('statistics','read_poster_report_sql_get',array($sqlStatistic,'date',date("Ym",strtotime($date))));
	return $detailList[$date]['sum']-$detailList[$dateP]['sum'];
}
/**
 * 高嫌疑数据进一步筛选，去除无效判定
 * @param 高嫌疑数据 $juagelist
 * @return 筛选结果
 */
function depth_filter($juagelist)
{
	$lists = array();
	$list = array();
	$pcList = array();
	foreach ($juagelist as $k=>$v)
	{
		if(!empty($v['detail']['ipMax']))
		{
			$ip = explode('-', $v['detail']['ipMax']);
			if(($ip[1]<25)&&(empty($v['detail']['pc'])))
			{
				continue;
			}			
			else if(!empty($v['detail']['pc']))
			{
				if($ip[1]*$v['detail']['pc']<800)
					continue;
			}
		}
		if($v['degree']<10)
			continue;
		$reason = '';
		
		foreach ($v['detail'] as $reason_name=>$reason_comment)
		{
			$reason .= $reason_name.":".$reason_comment."|";
			if($reason_name =='pc')
			{
				$pcList[] = $v['twitter'];
			}
		}
		$list = array('twitter_id'=>$v['twitter'],
		'pvs'=>empty($v['pvs'])?0:$v['pvs'],
				'pvs_past'=>(int)$v['pvs_past'],
				'gmv'=>(int)$v['gmv_now'],
				'gmv_past'=>(int)$v['gmv_past'],
				'total_order'=>(int)$v['total_order'],
				'time'=>$v['time'],
				'reason'=>$reason,
				'degree'=>(int)$v['depth'],
				'probably_action' => $v['probably_action'],
				'ip_total_count'=>-1,
				'ip_count' =>-1,
				'ip_max'=>'',
				'mac_total_count'=>-1,
				'mac_count'=>-1,
				'mac_max'=>'',
				'imei_total_count'=>-1,
				'imei_count'=>-1,
				'imei_max'=>'',
				'idfv_total_count'=>-1,
				'idfv_count'=>-1,
				'idfv_max'=>'',
				'user_total_count'=>-1,
				'user_count'=>-1,
				'user_max'=>'',
				'pc_rate'=>-1,
				'time_group'=>-1
				
		);
		foreach ($v['every'] as $k=>$v2)
		{
			$list[$k] = $v2;
		}
		#print_r($list);
		$lists[$v['twitter']] = $list;
	}
	
	return array($lists,$pcList);
}
/**
 * 数据库写入
 * @param 需要写入数据库的数据列表 $lists
 */
function data_write($lists)
{
	global $dateToCheck;
	/*foreach ($lists[0] as $list){
		$date = date('Y-m-d',$list['time']);
		$report_db = new PDO($dsn,$username,$password);
		$report_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
		$sql = "insert into t_malicious_report (twitter_id,pvs,pvs_past,gmv,gmv_past,total_order,
				reason,degree,probably_action,ip_total_count,
				ip_count,ip_max,mac_total_count,mac_count,mac_max,imei_total_count,imei_count,imei_max,
		idfv_total_count,idfv_count,idfv_max,user_total_count,user_count,user_max,pc_rate,time_group,createtime) values 
				({$list['twitter_id']},{$list['pvs']},{$list['pvs_past']},{$list['gmv']},{$list['gmv_past']},
		{$list['total_order']},'{$list['reason']}',{$list['degree']},'',{$list['ip_total_count']},{$list['ip_count']},
		'{$list['ip_max']}',{$list['mac_total_count']},{$list['mac_count']},'{$list['mac_max']}',{$list['imei_total_count']},{$list['imei_count']},
		'{$list['imei_max']}',{$list['idfv_total_count']},{$list['idfv_count']},'{$list['idfv_max']}',{$list['user_total_count']},{$list['user_count']},
		'{$list['user_max']}',{$list['pc_rate']},{$list['time_group']},'{$date}')";
		try{$sth = $report_db->prepare($sql);
		$sth->execute();}
		catch (Exception $e)
		{
			echo $e."\n";
			echo mysql_error();
		}
	}*/
	
	foreach ($lists as $pcList){
		$date = date('Y-m-d',$dateToCheck);
		$writer = dbAccess::getDb();
		$colum = array('twitter_id','pvs','degree','ip_group','user_group','ip_max','user_max','pc_rate','time_deviation','createtime');
		$value = array($pcList['twitter_id'],$pcList['pvs'],$pcList['degree'],$pcList['ip_group'],empty($pcList['user_group'])?0:$pcList['user_group'],
		(String)$pcList['ip_max'],(String)$pcList['user_max'],
		$pcList['pc_rate'],$pcList['time_deviation'],(String)$date);
		echo $writer->write(0, $colum, $value);
	}
}
juage($dateToCheck);


?>
