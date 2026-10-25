<?php
mysqli_report(MYSQLI_REPORT_ERROR|MYSQLI_REPORT_STRICT);
$m=new mysqli('127.0.0.1','root','','wiki',33362);
$weights=[];
for($start=0;$start<65536;$start+=256){
 $queries=[];
 for($cp=$start;$cp<$start+256;$cp++){
  if($cp>=0xd800&&$cp<=0xdfff)continue;
  $hex=bin2hex(mb_chr($cp,'UTF-8'));
  $queries[]="SELECT $cp AS cp,HEX(WEIGHT_STRING(CONVERT(UNHEX('$hex') USING utf8mb4) COLLATE utf8mb4_general_ci)) AS w";
 }
 if(!$queries)continue;
 foreach($m->query(implode(' UNION ALL ',$queries))->fetch_all(MYSQLI_ASSOC) as $row){
  if(strlen($row['w'])!==4)throw new Exception('unexpected weight '.$row['cp'].' '.$row['w']);
  $weight=hexdec($row['w']);if($weight!==(int)$row['cp'])$weights[$row['cp']]=$weight;
 }
}
$supplement=[];
foreach([0x10000,0x1f600,0x10ffff] as $cp){$hex=bin2hex(mb_chr($cp,'UTF-8'));$supplement[$cp]=$m->query("SELECT HEX(WEIGHT_STRING(CONVERT(UNHEX('$hex') USING utf8mb4) COLLATE utf8mb4_general_ci))")->fetch_row()[0];}
file_put_contents($argv[1],json_encode(['version'=>$m->server_info,'collation'=>'utf8mb4_general_ci','weights'=>$weights,'supplementary'=>$supplement],JSON_PRETTY_PRINT|JSON_THROW_ON_ERROR)."\n");
