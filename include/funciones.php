<?php
require_once("db_funcs.php");
$verlinks_abm = false;

function getParametroDB($nombre, $tipo) {
	$query="select numero, fecha, string from sig_parametros where parametro = '".$nombre."'";
	$result = db_query($query);
	$numberOfRows = MYSQL_NUM_ROWS($result);
	if ($numberOfRows>0) {
		$numero = MYSQL_RESULT($result, 0, "numero");
		$fecha = MYSQL_RESULT($result, 0, "fecha");
		$string = MYSQL_RESULT($result, 0, "string");
	}
	switch ($tipo) {
    case "numero":
        $res = $numero;
        break;
    case "fecha":
        $res = $fecha;
    	break;
    case "string":
        $res = $string;
    	break;
	}
	return $res;	 
}
function highlightSearchTerms($fullText, $searchTerm, $bgcolor="#FFFF99"){
	if (empty($searchTerm))
	{
		return $fullText;
	}
	else
	{
		$start_tag = "<span style=\"background-color: $bgcolor\">";
		$end_tag = "</span>";
		$highlighted_results = $start_tag . $searchTerm . $end_tag;
		return eregi_replace($searchTerm, $highlighted_results, $fullText);
	}
}
function ImprimirArrayVariables($var) {

	while (list ($key, $val) = each ($var)) {
		if ($val == "") $val = "NULL";
		$arVals[$key] = (get_magic_quotes_gpc()) ? stripslashes($val) : $val;
		$arVals[$key] = "'".$arVals[$key]."'";
		if ($debug) echo $key . " : [" . $arVals[$key] . "]<br>";
	}

}

function cambiarComaPorPunto($cadena){
	$coma   = ',';
	$pos = strpos($cadena, $coma);
	if ($pos !== false) {
		$cadena = str_ireplace(",", ".", $cadena);
	}
	return $cadena;
	
}

function convertirperiodo($periodo, $formatoinput, $formatooutput) {
	if( $periodo == '') return "";
	
	if($formatoinput == 'mm/aaaa' && $formatooutput=='aaaamm') {
		$html = substr($periodo, 3, 4)."/".substr($periodo, 0, 2);
	} elseif ($formatoinput == 'aaaamm' && $formatooutput == 'mm/aaaa') {
		$html = substr($periodo, 4, 2)."/".substr($periodo, 0, 4);
	}
	return $html;
}


?>
