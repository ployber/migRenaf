<?php
include("include/db_funcs.php"); 
include("include/funciones.php");

define("DEBUG", 1);
$listaTablas = Array(
		"act_agricultura_act_agricultura_detalle",
		"act_agricultura_detalle",
		"act_agroindustria_act_agroindustria_detalle",
		"act_agroindustria_detalle",
		"act_apicultura_act_apicultura_detalle",
		"act_apicultura_detalle",
		"act_artesania_act_artesania_detalle",
		"act_artesania_detalle",
		"act_caza_act_caza_detalle",
		"act_caza_detalle",
		"act_pastoreo_act_pastoreo_detalle",
		"act_pastoreo_detalle",
		"act_pastoreo_detalle_sub_producto_animal",
		"act_pesca_act_pesca_detalle",
		"act_pesca_detalle",
		"act_produccion",
		"act_recoleccion_act_recoleccion_detalle",
		"act_recoleccion_detalle",
		"act_turismo_rural_act_turismo_rural_detalle",
		"act_turismo_rural_detalle",
		"actividad_complementaria",
		"actividad_completa",
		"actividad_completa_actividad_complementaria",
		"actividad_completa_actividad_principal",
		"actividad_completa_adicionales",
		"actividad_principal",
		"cantidad_trabajadores_temporarios",
		"contrata_maquinaria",
		"distancia_viviendaaeducacion",
		"domicilio",
		"emergencia",
		"explotacion_con_limites",
		"explotacion_sin_limites",
		"integrante",
		"limite",
		"limite_explotacion_con_limites",
		"limite_explotacion_sin_limites",
		"mano_de_obra_completa",
		"mejora_produccion",
		"naf_completo",
		"naf_completo_integrante",
		"persona",
		"planilla_completa",
		"prevencion",
		"rec_embarcaciones",
		"rec_riego",
		"rec_tractores",
		"rec_vehiculo",
		"recursos",
		"riesgo",
		"salud_detalle",
		"sub_producto_animal",
		"superficie_completa",
		"tecnologia",
		"tierra",
		"tierra_prevencion",
		"tierra_riesgo",
		"tierra_tipo_juridico",
		"tipo_juridico",
		"titular_completa",
		"vivienda_detalle",
		"vivienda_detalle_agua_de_otros_origenes"
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

vaciar_tablas($Tconn, $listaTablas);

$Tconn->autocommit(TRUE);

$sql = "CREATE TABLE IF NOT EXISTS log_proceso (id bigint(20) NOT NULL AUTO_INCREMENT, clave varchar(255) NOT NULL,  formulario bigint(20) NOT NULL, estado varchar(10) NOT NULL,  obs varchar(255) NOT NULL,  PRIMARY KEY (id) ) ENGINE=InnoDB DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;";
if (!$Sconn->query($sql)) {
	pdberror($Sconn, $sql."\n"."CREATE TABLE log_proceso failed: ");echoif("\n\n");
}

vaciar_tablas($Sconn, Array("log_proceso"));



$Tconn->commit();
$Sconn->commit();
echo "FIN!\n";

/*******************************/
/*******************************/
/*******************************/
/*  F I N    */
/*******************************/
/*******************************/
/*******************************/

function vaciar_tablas($conn, $listaTablas) {
	$conn->query("SET foreign_key_checks = 0;");
	echoif("borrando tablas...\n");
	foreach ($listaTablas as $tabla) {
		$del_query = "delete FROM ".$tabla.";"; 
		echo $del_query."\n";
		if (!$conn->query($del_query)) {
			pdberror($conn, $del_query." failed: ");
			echoif("\n\n");
		} else {
			echoif("Se borro ok ".$tabla."\n");
		}
		$conn->commit();
	}
	$conn->query("SET foreign_key_checks = 1;");
	echoif("Listo borrar tablas\n\n");
	return;
}
 
?>