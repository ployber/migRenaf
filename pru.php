<?php
include("include/db_funcs.php"); 
include("include/funciones.php");

define("DEBUG", 0);

$unidades_infraestructura = Array(
	"0"=>"0",
	"8022"=>"u",
	"8024"=>"m",
	"8025"=>"m",
	"8023"=>"m",
	"8027"=>"u",
	"8030"=>"u",
	"8029"=>"u",
	"8018"=>"u",
	"8019"=>"u",
	"8007"=>"m2",
	"8013"=>"m",
	"8011"=>"m2",
	"8032"=>"m2",
	"8012"=>"m2",
	"8001"=>"m2",
	"8004"=>"m2",
	"8002"=>"m2",
	"8003"=>"m2",
	"8006"=>"m2",
	"8009"=>"m2",
	"8033"=>"m2",
	"8026"=>"u",
	"8028"=>"u",
	"8017"=>"u",
	"8031"=>"u",
	"8037"=>"m3",
	"8034"=>"m2",
	"8035"=>"u",
	"8036"=>"m3",
	"8010"=>"m2",
	"8021"=>"u",
	"8016"=>"m2",
	"8014"=>"t/h",
	"8020"=>"l",
	"8015"=>"l",
	"8008"=>"m2"
);
$dbHostname = "localhost";
$dbUsername = "root";
$dbPassword = "feli0806";
$Sconn = new mysqli($dbHostname, $dbUsername, $dbPassword, SOURCEDB);
if (mysqli_connect_errno($Sconn)) {
	pdberror($Sconn, "Failed to connect to MySQL: " . mysqli_connect_error($Sconn));
	echoif("\n\n");
} else {
	 echoif("Connection OK con ".SOURCEDB."!\n");
}

$Tconn = new mysqli($dbHostname, $dbUsername, $dbPassword, TARGETDB);
if (mysqli_connect_errno($Tconn)) {
	pdberror($Tconn, "Failed to connect to MySQL: " . mysqli_connect_error($Tconn));
	echoif("\n\n");
} else {
	echoif("Connection OK con ".TARGETDB."!\n");
}

$Tconn->autocommit(FALSE);

$sel = "SELECT
ifnull(p.InfraestructuraCodigo, '') InfraestructuraCodigo,
ifnull(p.InfraestructuraCantidad, 0) InfraestructuraCantidad,
ifnull(p.InfraestructuraAdquisicion, '0') InfraestructuraAdquisicion,

ifnull(a.Codigo, ifnull(p.InfraestructuraCodigo, '')) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(v.Codigo, ifnull(p.InfraestructuraAdquisicion, '')) adq_cod,
ifnull(v.Descripcion, '') adq_desc

from infraestructura p
left join tipoinfraestructura a on p.InfraestructuraCodigo = a.Codigo
left join tipoadquisicion v on p.InfraestructuraAdquisicion = v.Codigo";

$res_titulares = $Sconn->query($sel);
if ($Sconn->errno) {
	$errors[] = $Sconn->error;
    printf("error : %s\n", $Sconn->errno);
	print_r($errors);
} 
$nRows_titulares = $Sconn->affected_rows;
echoif($nRows_titulares. " filas\n\n");
if ($nRows_titulares > 0) {
	$i=0;
	while ($row_tit = mysqli_fetch_array($res_titulares)) {
		$i++;
		if( !isset( $unidades_infraestructura[$row_tit['InfraestructuraCodigo']] ) ) {
			echo $i."- [".$row_tit['InfraestructuraCodigo']."][".$row_tit['act_cod']."][".$row_tit['act_desc']."]\n ";
		} else {
			echo $row_tit['adq_cod']." ";
			echo $row_tit['adq_desc']." ";
			echo $unidades_infraestructura[$row_tit['act_cod']]." ";
			echo $unidades_infraestructura[$row_tit['InfraestructuraCodigo']]." ";
			echo $row_tit['act_desc']."\n";
		}
	}
}