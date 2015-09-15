<?php
/**
 * @author zhiganggao
 */
ini_set("display_errors",'On');
date_default_timezone_set('Asia/Shanghai');
require_once '/home/brdwork/data/release/brdpro8/brdlib/stdafx.php';//brdlib入口类

$dateToCheck = strtotime(date('Y-m-d',time()-2*86400));//查询时间定义，默认昨天

class dbAccess
{
	private $dsn = 'mysql:host=172.16.0.188;dbname=test;port=3316';
	private $username = 'mlsreader';
	private $password = 'RMlSxs&^c6OpIAQ1';
	private static $db = null;
	private $platform = array('_pc','_mob','');
	private $report_db = null;
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
		$sql = 'insert into t_malicious_report_shows'.$this->platform[$platform].' ('.$columString.") values (".$valueString.")";
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

function writeFile($showHeader,$outData)
{
    $file_path = '/home/brdwork/data/download/';
    $filename = $file_path . md5(date('YmdH')) . '_malicious_analysis_shows.csv';
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
function showsSummry($dateToCheck)
{
	$date = date('Y-m-d',$dateToCheck);
	$where = "createtime = '".$date."' and shows > 1000 order by shows desc";
	$dbreader = dbAccess::getDb();
	$data = $dbreader->readShows(0, array('shows','twitter_id'), $where);
	$log = array();
	foreach ($data as $v)
	{
		$log[$v['twitter_id']]=$v['shows'];
	
    }
	return $log;
}

function juage($dateToCheck)
{
    //$checkarray = array('ip','imei','mac','idfv','user');
	$checkarray = array('ip','user_id');
	$dbreader = dbAccess::getDb();
	$result = showsSummry($dateToCheck);
	$count11 = 0;
	foreach ($result as $k=>$v2)
	{
		$date = date('Y-m-d',$dateToCheck);
		$selectColumn = array('ip_total_count','ip_count','ip_max','user_id_total_count','user_id_count','user_id_max','pc_rate','time_deviation','pc_rate');
		$where = "createtime = '".$date."' and twitter_id = '".$k."'";
		$data = $dbreader->readShows(0, $selectColumn, $where);
		$row = $data[0];
		$item = array();
		$degree = 0;
		foreach ($checkarray as $v)
		{
			$group = $row[$v.'_count']/$row[$v.'_total_count']*100;
			$maxIndex = explode("|",$row[$v.'_max']);
			$item[$v.'_group'] = (float)$group;
			$item[$v.'_max'] = $row[$v.'_max'];
			if(!empty($group) && !empty($maxIndex[1])){
				if($group >= 30)
				{
					$degree += 80/7 - (4/35) * $group;
				}
				else
				{
					$degree += 20 - (2/5) * $group;
				}
			}
			
			if($maxIndex[1] >= 200)
				$degree += 20/pow(2,1000/$maxIndex[1]);
		}
		$item['pc_rate'] = (float)$row['pc_rate'] *100;
		if(!empty($item['pc_rate']) && $item['pc_rate']>0)
			$degree += log(pow(2,(($item['pc_rate']/100)*($item['pc_rate']/100))))*29;
		if($row['time_deviation'] > 1.6)
			$degree += 40/pow(2,pow(2,9/(4*($row['time_deviation']-0.8)*($row['time_deviation']-0.8))));
		$item['twitter_id'] = $k;
		$item['time_deviation'] = (float)$row['time_deviation'];
		$item['shows'] = (int)$v2;
		$item['degree'] = (int)$degree;
		$keys = array_keys($item);
		$values = array_values($item);
		$keys[] = 'createtime';
		$values[] = date('Y-m-d',$dateToCheck);
		echo $dbreader->write(0, $keys, $values)."\n";
	}
/*$showHeader = array(
        'twitter_id'=>array('show'=>'推ID','class'=>'c50'),
            'shows'=>array('show'=>'展示','class'=>'c50'),
            'ip_group'=>array('show'=>'ip集群','class'=>'c50'),
            'ip_max'=>array('show'=>'ip最大访问','class'=>'c50'),
            'user_id_group'=>array('show'=>'user_id集群','class'=>'c50'),
            'user_id_max'=>array('show'=>'user_id最大访问','class'=>'c50'),
            'time_deviation'=>array('show'=>'时间偏差','class'=>'c50'),
            'pc_rate'=>array('show'=>'pc端比例','class'=>'c50'),
            'degree'=>array('show'=>'分数','class'=>'c50'),
    );*/
    //data_write(depth_filter($juageList));
    //writeFile($showHeader, $juageList);
}
juage($dateToCheck);
