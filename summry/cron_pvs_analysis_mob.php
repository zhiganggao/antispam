<?php
/**
 * @author zhiganggao
*/
//ini_set("display_errors",'On');
date_default_timezone_set('Asia/Shanghai');
require_once '/home/brdwork/data/release/brdpro8/brdlib/stdafx.php';//brdlib入口类

$dateToCheck = strtotime(date('Y-m-d',time()-3*86400));//查询时间定义，默认昨天

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
 *高点击twi列表提取
 * @param 查询时间 $dateToCheck
 * @return 高点击twi列表提取
 */
function pvsSummry($dateToCheck)
{
	$date = date('Y-m-d',$dateToCheck);
	$where = "createtime = '".$date."' order by pvs desc";
	$dbreader = dbAccess::getDb();
	$data = $dbreader->readPvs(1, array('pvs','twitter_id'), $where);
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
	$log = pvsSummry($dateToCheck);
	foreach ($log as $k=>$v)
	{
		if($v<100)
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
	foreach ($order as $k => $v)
	{
		$outdata[] = array('twitter_id'=>$k,'pvs'=>$log[$k],'createOrder'=>$v['createOrder'],'notPayOrder'=>$v['notPayOrder'],
				'rate'=>$v['rate']);
	}
	return $outdata;
}

/**
 * 对高风险列表数据进行判断，确定其是否真的有风险嫌疑，并获取其嫌疑，生成每天的嫌疑列表
 * @param unknown $dateToCheck
 */
function juage($dateToCheck)
{
	$juageList = array();
	$checkarray = array('ip','imei','mac','idfv','user');
	//$checkarray = array('ip','user');
	$dbreader = dbAccess::getDb();
	$result = allSummry($dateToCheck);
	foreach ($result as $k=>$v2)
	{
		$date = date('Y-m-d',$dateToCheck);
		$selectColumn = array('ip_total_count','ip_count','ip_max','user_total_count','user_count','user_max','mac_total_count','mac_count','mac_max','imei_total_count','imei_count','imei_max','idfv_total_count','idfv_count','idfv_max','time_deviation','illegal_count');
		$where = "createtime = '".$date."' and twitter_id = ".$v2['twitter_id'];
		$data = $dbreader->readPvs(1, $selectColumn, $where);
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
				$degree += log(pow(2,(1-$group)*(1-$group)))*20;
			if($maxIndex[1] >= 20)
				$degree += 10/pow(2,100/$maxIndex[1]);
		}
		if($row['time_deviation'] > 0.8)
			$degree += 25/pow(2,pow(2,9/(4*$row['time_deviation']*$row['time_deviation'])));
		$item['twitter_id'] = $v2['twitter_id'];
		$item['time_deviation'] = $row['time_deviation'];
		$item['pvs'] = $v2['pvs'];
		$item['degree'] = $degree;
		$juageList[] = $item;

	}
	data_write($juageList);

}

/**
 * 数据库写入
 * @param 需要写入数据库的数据列表 $lists
 */
function data_write($lists)
{
	global $dateToCheck;

	foreach ($lists as $mobList){
		$date = date('Y-m-d',$dateToCheck);
		$writer = dbAccess::getDb();
		$colum = array('twitter_id','pvs','degree','ip_group','user_group','mac_group','imei_group','idfv_group','ip_max','user_max','mac_max','imei_max','idfv_max','time_deviation','createtime');
		$value = array($mobList['twitter_id'],$mobList['pvs'],$mobList['degree'],$mobList['ip_group'],empty($mobList['user_group'])?0:$mobList['user_group'],empty($mobList['mac_group'])?0:$mobList['mac_group'],empty($mobList['imei_group'])?0:$mobList['imei_group'],empty($mobList['idfv_group'])?0:$mobList['idfv_group'],
				(String)$mobList['ip_max'],(String)$mobList['user_max'],(String)$mobList['mac_max'],(String)$mobList['imei_max'],(String)$mobList['idfv_max'],
				$mobList['time_deviation'],(String)$date);
		echo $writer->write(1, $colum, $value);
	}
}
juage($dateToCheck);
