<?php
include("include/db_funcs.php"); 
include("include/funciones.php");

define("DEBUG", 0);

$Errores = Array(
	"persona"=>0,
	"titular_completa"=>0,
	"integrante"=>0,
	"naf_completo"=>0,
	"domicilio"=>0,
	"naf_completo_integrante"=>0,
	"actividad_completa"=>0,
	"actividad_complementaria"=>0,
	"actividad_completa_actividad_complementaria"=>0,
	"actividad_principal"=>0,
	"actividad_completa_actividad_principal"=>0,
	"act_agricultura_detalle"=>0,
	"act_agricultura_act_agricultura_detalle"=>0,
	"act_agroindustria_detalle"=>0,
	"act_agroindustria_act_agroindustria_detalle"=>0,
	"act_apicultura_detalle"=>0,
	"act_apicultura_act_apicultura_detalle"=>0,
	"act_artesania_detalle"=>0,
	"act_artesania_act_artesania_detalle"=>0,
	"act_caza_detalle"=>0,
	"act_caza_act_caza_detalle"=>0,
	"act_pesca_detalle"=>0,
	"act_pesca_act_pesca_detalle"=>0,
	"act_recoleccion_detalle"=>0,
	"act_recoleccion_act_recoleccion_detalle"=>0,
	"act_turismo_rural_detalle"=>0,
	"act_turismo_rural_act_turismo_rural_detalle"=>0,
	"act_pastoreo_detalle"=>0,
	"act_pastoreo_act_pastoreo_detalle"=>0,
	"sub_producto_animal"=>0,
	"act_produccion"=>0,
	"act_pastoreo_detalle_sub_producto_animal"=>0
);

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
				"act_pesca_act_pesca_detalle",
				"act_pesca_detalle",   
				"act_recoleccion_act_recoleccion_detalle",
				"act_recoleccion_detalle",   
				"act_turismo_rural_act_turismo_rural_detalle",
				"act_turismo_rural_detalle",   
				"act_pastoreo_detalle_sub_producto_animal",
				"sub_producto_animal",   
				"act_pastoreo_act_pastoreo_detalle",
				"act_pastoreo_detalle",   
				"actividad_completa_actividad_complementaria",
				"actividad_complementaria",  
				"actividad_completa_actividad_principal",
				"actividad_principal",   
				"actividad_completa_adicionales",
				"mejora_produccion",
				"rec_embarcaciones",
				"rec_tractores",
				"rec_vehiculo",
				"recursos",
				"rec_riego",
				"limite_explotacion_con_limites",
				"explotacion_con_limites",
				"limite_explotacion_sin_limites",
				"explotacion_sin_limites",
				"distancia_viviendaaeducacion",
				"salud_detalle",
				"emergencia",
				"superficie_completa",
				"auditoria_planilla_reducida",
				"planilla_reducida_action_alertado",
				"transicionfid_planilla_reducida",
				"planilla_reducida",
				"naf_sector",
				"naf_completo_integrante",
				"integrante",
				"planilla_completa_action_alertado",
				"transicionpr_planilla_completa",
				"auditoria_planilla_completa",
				"planilla_completa",
				"naf_completo",
				"actividad_completa",
				"titular",
				"titular_completa",
				"persona"
				);


$unidades_infraestructura = Array(
	"8022"=>"8",	"8024"=>"2",	"8025"=>"2",	"8023"=>"2",
	"8027"=>"8",	"8030"=>"8",	"8029"=>"8",	"8018"=>"8",
	"8019"=>"8",	"8007"=>"4",	"8013"=>"2",	"8011"=>"4",
	"8032"=>"4",	"8012"=>"4",	"8001"=>"4",	"8004"=>"4",
	"8002"=>"4",	"8003"=>"4",	"8006"=>"4",	"8009"=>"4",
	"8033"=>"4",	"8026"=>"8",	"8028"=>"8",	"8017"=>"8",
	"8031"=>"8",	"8037"=>"9",	"8034"=>"4",	"8035"=>"8",
	"8036"=>"9",	"8010"=>"4",	"8021"=>"8",	"8016"=>"4",
	"8014"=>"12",	"8020"=>"11",	"8015"=>"11",	"8008"=>"4"
);


switch ($argc) {
	case 1;
		break;
	case 2;
		$arg1 = $argv[1];
		break;
	case 3;
		$arg1 = $argv[1];
		$arg2 = $argv[2];
		break;
}

$DESDE="1";
$TOPEFILASAPROCESAR=99999;
if (empty($arg2)) {
	if (!empty($arg1)) {
		if (is_numeric($arg1)) {
			$TOPEFILASAPROCESAR = $arg1;
		}
	}
} else {
	/* desde hasta */
	if (is_numeric($arg1)) {
		$DESDE = $arg1;
	}
	if (is_numeric($arg2)) {
		$TOPEFILASAPROCESAR = $arg2;
	}
}
echo "DESDE: ".$DESDE."\n";
echo "TOPE : ".$TOPEFILASAPROCESAR."\n";
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

$Tconn->commit();

$sel_titulares = select_titulares();

echoif("Comienzo a leer titulares\n\n");
$res_titulares = $Sconn->query($sel_titulares);
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
		//print_r($row_tit);
		$i++;

		$k1 = $row_tit['Ventanillaregistro']; 
		$k2 = $row_tit['pc']; 
		$k3 = $row_tit['Correlativo'];
		
		if ($i > $TOPEFILASAPROCESAR) {
			//$result->close();
			break;
		}
		if (!empty($DESDE)) {
			if ($i < $DESDE -1) {
				continue;
			}
		}
		echoif("row ".$i." Key:[".$k1."-".$k2."-".$k3."]");
		echoif(" Titular :[".$row_tit['TipoDocumento']."][".$row_tit['NumeroDocumento']."]");
		echoif(" Conyuge :[".$row_tit['CyTipoDocumento']."][".$row_tit['CyNumeroDocumento']."]\n");
		$n_TipoDocumento = intval($row_tit['TipoDocumento']);
		$n_NumeroDocumento = floatval($row_tit['NumeroDocumento']);
		$n_CyTipoDocumento = intval($row_tit['CyTipoDocumento']);
		$n_CyNumeroDocumento = floatval($row_tit['CyNumeroDocumento']);
		
		
		if (($i % 100) == 0 ) {
			echo "row ".$i." Key:[".$k1."-".$k2."-".$k3."]\n";
		}

		if ($row_tit['NumeroDocumento'] == "") {
			//salto a la fila siguiente
			echoif(" sin datos del titular, ignoro registro.\n\n");
			echoif("\n\n\n");
			continue;
		}
		if ($n_NumeroDocumento < 10000) {
			//salto a la fila siguiente
			echoif(" Titular con nro documento invalido: ".$n_NumeroDocumento.", ignoro registro.\n\n");
			echoif("\n\n\n");
			continue;
		}
				
		$id_titular = -1;
		$id_conyuge = -1;
		$id_tit_completa_tit = -1;
		$id_tit_completa_cy = -1;
		
		
		/* Si cualquiera de los dos ya existe en la tabla personas NO proceso el registro */
		/* existe titular */
		$existe_titular=0;
		$sql  = " SELECT * FROM ".SOURCEDB.".titulares t ";
		$sql .= " LEFT JOIN ".TARGETDB.".tipo_documento d ON ifnull( t.TipoDocumento, '-1' ) = d.codigo ";
		$sql .= " JOIN ".TARGETDB.".persona p ON p.documento = t.NumeroDocumento ";
		$sql .= " AND tipo_documento_id = d.id ";
		$sql .= " WHERE t.NumeroDocumento = '".$row_tit['NumeroDocumento']."'";
		$sql .= " AND t.TipoDocumento = '".$row_tit['TipoDocumento']."'";
		$res_existe_titular = $Sconn->query($sql);
		echoif($sql."\n");
		if ($Sconn->errno) {
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sql."\n";
			print_r($errors);
		} else { 
			$nRows_existe_titular = $Sconn->affected_rows;
			if ($nRows_existe_titular > 0) {
				$row_existe_titular = mysqli_fetch_array($res_existe_titular);
				if ($row_tit['NumeroDocumento'] == $row_existe_titular['documento']) {
					$existe_titular=1;
				}
			}
		}
		
		/* existe_conyuge */
		$existe_conyuge=0;
		$sql  = " SELECT * FROM ".SOURCEDB.".titulares t ";
		$sql .= " LEFT JOIN ".TARGETDB.".tipo_documento d ON ifnull( t.CyTipoDocumento, '-1' ) = d.codigo ";
		$sql .= " JOIN ".TARGETDB.".persona p ON p.documento = t.CyNumeroDocumento ";
		$sql .= " AND tipo_documento_id = d.id ";
		$sql .= " WHERE t.CyNumeroDocumento = '".$row_tit['CyNumeroDocumento']."'";
		$sql .= " AND t.CyTipoDocumento = '".$row_tit['CyTipoDocumento']."'";
		$res_existe_conyuge = $Sconn->query($sql);
		echoif($sql."\n");
		if ($Sconn->errno) {
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sql."\n";
			print_r($errors);
		} else { 
			$nRows_existe_conyuge = $Sconn->affected_rows;
			if ($nRows_existe_conyuge > 0) {
				$row_existe_conyuge = mysqli_fetch_array($res_existe_conyuge);
				if ($row_tit['CyNumeroDocumento'] == $row_existe_conyuge['documento']) {
					$existe_conyuge=1;
				}
			}
		}
		echoif("[".$existe_titular."][".$existe_conyuge."]\n");

		if ($existe_titular || $existe_conyuge) {
			//salto a la fila siguiente
			if ($existe_titular) {
				echoif("Ya existe el titular en la tabla de personas ".$row_tit['TipoDocumento']."-".$row_tit['NumeroDocumento']);
			}
			if ($existe_conyuge) {
				echoif("Ya existe el conyuge en la tabla de personas ".$row_tit['CyTipoDocumento']."-".$row_tit['CyNumeroDocumento']);
			}
			echoif("\nignoro registro.\n\n");
			echoif("\n\n\n");
			continue;
		}

/*
		echo "Titular :[".$row_tit['TipoDocumento']."][".$row_tit['NumeroDocumento']."]\n";
		echo "Titular :[".$n_TipoDocumento."][".$n_NumeroDocumento."]\n";
		echo "Conyuge :[".$row_tit['CyTipoDocumento']."][".$row_tit['CyNumeroDocumento']."]\n";
		echo "Conyuge :[".$n_CyTipoDocumento."][".$n_CyNumeroDocumento."]\n";

		if ($row_tit['CyNumeroDocumento'] != "" && $row_tit['CyNumeroDocumento'] != "0") {
			$SINCONYUGE=0;
		} else {
			$SINCONYUGE=1;
		}
*/

		if ($n_CyNumeroDocumento > 0) {
			$SINCONYUGE=0;
		} else {
			$SINCONYUGE=1;
		}
		/* documentos del titular y el conyuge iguales no cargo conyuge */
		if (($row_tit['NumeroDocumento'] == $row_tit['CyNumeroDocumento']) &&
			$row_tit['TipoDocumento'] == $row_tit['CyTipoDocumento'] ) {
				$SINCONYUGE=1;
		}
		if ($n_CyTipoDocumento < 10000) {
			$SINCONYUGE=1;
		}

		$sel_tierra = select_tierra($k1, $k2, $k3);
		$res_tierra = $Sconn->query($sel_tierra);
		if ($Sconn->errno) {
			$nRows_tierra = 0;
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sel_tierra."\n";
			print_r($errors);
		} else { 
			$nRows_tierra = $Sconn->affected_rows;
		}
		if ($nRows_tierra > 0) {
			$row_tierra = mysqli_fetch_array($res_tierra);		
		} else {
			$row_tierra = array();
		}
		echoif("Obtenido registro tierra ".$nRows_tierra."\n\n");
				
		$sel_pv = select_prodvegetal($k1, $k2, $k3); 
		$res_pv = $Sconn->query($sel_pv);
		if ($Sconn->errno) {
			$nRows_pv = 0;
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sel_pv."\n";
			print_r($errors);
		} else { 
			$nRows_pv = $Sconn->affected_rows;
			echoif("Obtenidos registros prodvegetal ".$nRows_pv."\n");
		}

		$sel_pa = select_prodanimal($k1, $k2, $k3);
		$res_pa = $Sconn->query($sel_pa);
		if ($Sconn->errno) {
			$nRows_pa = 0;
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sel_pa."\n";
		    print_r($errors);
		} else { 
			$nRows_pa = $Sconn->affected_rows;
			echoif("Obtenidos registros prodanimal ".$nRows_pa."\n");
		}

		$sel_ar = select_artesanias($k1, $k2, $k3);
		$res_ar = $Sconn->query($sel_ar);
		if ($Sconn->errno) {
			$nRows_ar = 0;
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sel_ar."\n";
			print_r($errors);
		} else { 
			$nRows_ar = $Sconn->affected_rows;
			echoif("Obtenidos registros artesanias ".$nRows_ar."\n");
		}

		$sel_in = select_infraestructura($k1, $k2, $k3);
		$res_in = $Sconn->query($sel_in);
		if ($Sconn->errno) {
			$nRows_in = 0;
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sel_in."\n";
			print_r($errors);
		} else { 
			$nRows_in = $Sconn->affected_rows;
			echoif("Obtenidos registros infraestructura ".$nRows_in."\n");
		}
			
		$sel_ai = select_agroindustria($k1, $k2, $k3); 
		$res_ai = $Sconn->query($sel_ai);
		if ($Sconn->errno) {
			$nRows_ai = 0;
			$errors[] = $Sconn->error;
		    printf("error : %s\n", $Sconn->errno);
			echo $sel_ai."\n";
			print_r($errors);
		} else { 
			$nRows_ai = $Sconn->affected_rows;
			echoif("Obtenidos registros agroindustria ".$nRows_ai."\n\n");
		}

		/*
		naf_completo
		has many titular_completo
				 has persona
		has many integrante
				 has persona
		*/	 		
		/* INSERT persona */
		echoif("titular\n");
		echoif(" tit_IdDocumento[".$row_tit['tdtit_id']."] tit_TipoDocumento[".$row_tit['TipoDocumento']."] tit_NumeroDocumento[".$row_tit['NumeroDocumento']."] tit_Nombres[".$row_tit['Nombres']."] tit_Apellido[".$row_tit['Apellido']."]\n");
		echoif(" Nivel educativo [".$row_tit['NivelEducativo']."]\n");
		echoif(" Nivel educativo Cy [".$row_tit['CyNivelEducativo']."]\n");
		echoif(" parentesco [".$row_tit['parentesco']."]\n");
		echoif(" Cyparentesco [".$row_tit['Cyparentesco']."]\n");
		
		$apellido = mysqli_real_escape_string($Tconn, $row_tit['Apellido']);
		$nombres = mysqli_real_escape_string($Tconn, $row_tit['Nombres']);
		$ins_titular = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
		$ins_titular .= " VALUES ( 0, '".$apellido."', '".$row_tit['NumeroDocumento']."', '".$row_tit['FechaNacimiento']."', 'Argentina', '".$nombres."', 'M', ".$row_tit['tdtit_id'].")";
		echoif($ins_titular."\n");
		if (!$Tconn->query($ins_titular)) {
			pdberror($Tconn, $ins_titular."\n"."INSERT persona (titular) failed: ");$Errores['persona']++;echoif("\n\n");
		} else {
			$id_titular = $Tconn->insert_id;
			echoif(" id persona:".$id_titular."\n\n");
		}
		if (empty($id_titular)) {
			$id_titular = "null";
		}

		echoif("conyuge\n");
		echoif(" cy_IdDocumento[".$row_tit['tdcy_id']."] cy_TipoDocumento[".$row_tit['CyTipoDocumento']."] cy_NumeroDocumento[".$row_tit['CyNumeroDocumento']."] cy_Nombres[".$row_tit['CyNombres']."] cy_Apellido[".$row_tit['CyApellido']."]\n");
				
		if (!$SINCONYUGE) {
			$apellido = mysqli_real_escape_string($Tconn, $row_tit['CyApellido']);
			$nombres = mysqli_real_escape_string($Tconn, $row_tit['CyNombres']);
			$ins_conyuge = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_conyuge .= " VALUES ( 0, '".$apellido."', '".$row_tit['CyNumeroDocumento']."', '".$row_tit['CyFechaNacimiento']."', 'Argentina', '".$nombres."', 'F', ".$row_tit['tdcy_id'].")";
			echoif($ins_conyuge."\n");
			if (!$Tconn->query($ins_conyuge)) {
				pdberror($Tconn, $ins_conyuge."\n"."INSERT persona (conyuge) failed: ");$Errores['persona']++;echoif("\n\n");
			} else {    
				$id_conyuge = $Tconn->insert_id;
				echoif(" id conyuge:".$id_conyuge."\n");
			}
			//$ins_result = Tdb_query($ins_conyuge);
		}
		if (empty($id_conyuge)) {
			$id_conyuge = "null";
		}
		/* Integrantes */
		echoif("Integrante titular\n");
		if ($row_tit['NumeroDocumento'] != "") {
			$ins_integrante = " INSERT INTO integrante (version, nivel_educativo_id, parentescoh_id, parentescom_id, persona_id, trabaja_en_naf) ";
			$ins_integrante .= " VALUES ( 0, ".trim($row_tit['niveled1_id']).", ".trim($row_tit['parentesco1_id']).", ".trim($row_tit['parentesco2_id']).", ".$id_titular.", b'".$row_tit['TrabajaEnElPredio']."')";
			echoif($ins_integrante."\n");
			if (!$Tconn->query($ins_integrante)) {
				pdberror($Tconn, $ins_integrante."\n"."INSERT integrante (titular) failed: ");$Errores['integrante']++;echoif("\n\n");
			} else {
				$id_integrante_tit = $Tconn->insert_id;
				echoif(" id integrante titular:".$id_integrante_tit."\n\n");
			}
		}
		if (empty($id_integrante_tit)) {
			$id_integrante_tit = "null";
		}

		echoif("Integrante conyuge\n");
		if (!$SINCONYUGE) {
			$ins_integrante = " INSERT INTO integrante (version, nivel_educativo_id, parentescoh_id, parentescom_id, persona_id, trabaja_en_naf) ";
			$ins_integrante .= " VALUES ( 0, ".trim($row_tit['niveled2_id']).", ".trim($row_tit['parentesco2_id']).", ".trim($row_tit['parentesco1_id']).", ".$id_conyuge.", b'".$row_tit['CyTrabajaEnElPredio']."')";
			echoif($ins_integrante."\n");
			if (!$Tconn->query($ins_integrante)) {
				pdberror($Tconn, $ins_integrante."\n"."INSERT integrante (conyuge) failed: ");$Errores['integrante']++;echoif("\n\n");
			} else {
				$id_integrante_cy = $Tconn->insert_id;
				echoif(" id integrante Conyuge:".$id_integrante_cy."\n\n");
			}
		}
		if (empty($id_conyuge)) {
			$id_conyuge = "null";
		}
		
 		/* INSERT titular_completa */
 		
 		$ins_tit_completa = " INSERT INTO titular_completa (version, cuit, persona_id) ";
		$ins_tit_completa .= " VALUES ( 0, '".trim($row_tit['CuitCuil'])."', ".$id_titular." )";
		echoif("titular_completa (titular)\n");
		echoif($ins_tit_completa."\n");
		if (!$Tconn->query($ins_tit_completa)) {
			pdberror($Tconn, $ins_tit_completa."\n"."INSERT titular_completa (titular) failed: ");$Errores['titular_completa']++;echoif("\n\n");
		} else {
			$id_tit_completa_tit = $Tconn->insert_id;
			echoif(" id titular_completa (titular):".$id_tit_completa_tit."\n");
		}
		if (!$SINCONYUGE) {
	 		$ins_tit_completa = " INSERT INTO titular_completa (version, cuit, persona_id) ";
			$ins_tit_completa .= " VALUES ( 0, '".trim($row_tit['CyCuitCuil'])."', ".$id_conyuge." )";
			echoif("titular_completa (conyuge)\n");
			echoif($ins_tit_completa."\n");
			if (!$Tconn->query($ins_tit_completa)) {
				pdberror($Tconn, $ins_tit_completa."\n"."INSERT titular_completa (conyuge) failed: ");$Errores['titular_completa']++;echoif("\n\n");
			}  else {
				$id_tit_completa_cy = $Tconn->insert_id;
				echoif(" id titular_completa (conyuge):".$id_tit_completa_cy."\n");
			}
		}
		
		/* INSERT naf_completo */
		$ins_naf_completo = " insert into naf_completo (version, titular1_id, titular2_id ) ";
		if (!$SINCONYUGE) {
			$ins_naf_completo .= " values ('0', ".$id_tit_completa_tit.", ".$id_tit_completa_cy.")";
		} else {
			$ins_naf_completo .= " values ('0', ".$id_tit_completa_tit.", null)";
		}
		echoif("naf_completo\n");
		echoif($ins_naf_completo."\n");
		if (!$Tconn->query($ins_naf_completo)) {
			pdberror($Tconn, $ins_naf_completo."\n"."INSERT naf_completo failed: ");$Errores['naf_completo']++;echoif("\n\n");
		} else {    
			$id_naf_completo = $Tconn->insert_id;
			echoif(" id naf_completo:".$id_naf_completo."\n");
		}

		/* naf_completo_integrante */
		$ins_naf_completo_integrante = " insert into naf_completo_integrante (naf_completo_integrantes_id, integrante_id) ";
		if (!$SINCONYUGE) {
			$ins_naf_completo_integrante .= " values (".$id_naf_completo.", ".$id_integrante_tit."), (".$id_naf_completo.", ".$id_integrante_cy.")";
		} else {
			$ins_naf_completo_integrante .= " values (".$id_naf_completo.", ".$id_integrante_tit.")";
		}
		echoif("naf_completo_integrante\n");
		echoif($ins_naf_completo_integrante."\n");
		if (!$Tconn->query($ins_naf_completo_integrante)) {
			pdberror($Tconn, $ins_naf_completo_integrante."\n"."INSERT naf_completo_integrante failed: ");$Errores['naf_completo_integrante']++;echoif("\n\n");
		}
		
		/* resto integrantes - familiares */
		ins_familiares($Sconn, $Tconn, $id_naf_completo, $k1, $k2, $k3);

		/* domicilio_id */
		$ins_domicilio = "INSERT INTO domicilio (version, datos_catastrales, departamento_id, direccion, localidad, municipio_distrito_comuna_id, otra_referencia, paraje, provincia_id, codigo_postal) ";
		$ins_domicilio .= "VALUES ('0', "; 
		$ins_domicilio .= "'' ,"; /* datos_catastrales */ 
		$ins_domicilio .= $row_tierra['Departamento']." ,"; /* departamento_id */
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['Domicilio'])."' ,"; /* direccion */
		 /* geo_lat, geo_lat_renaf, geo_long, geo_long_renaf, */ 
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['Localidad'])."' ,"; /* localidad */
		$ins_domicilio .= $row_tierra['Municipio']." ,"; /* municipio_distrito_comuna_id */ 
		/* nrookm */
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['OtraReferencia'])."' ,"; /* otra_referencia */ 
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['Paraje'])."' ,"; /* paraje */
		$ins_domicilio .= $row_tierra['Provincia']." ,"; /* provincia_id */ 
		$ins_domicilio .= "'".$row_tierra['CP']."' "; /* codigo_postal */
		$ins_domicilio .= ")";
		echoif("Domicilio\n");
		echoif($ins_domicilio."\n");
		$id_domicilio = 0;
		if (!$Tconn->query($ins_domicilio)) {
			pdberror($Tconn, $ins_domicilio."\n"."INSERT domicilio (persona) failed: ");$Errores['domicilio']++;echoif("\n\n");
		} else {    
			$id_domicilio = $Tconn->insert_id;
			echoif(" id domicilio:".$id_domicilio."\n");
		}

		/* domicilio_produccion_id */
		$ins_domicilio = "INSERT INTO domicilio (version, datos_catastrales, departamento_id, direccion, localidad, municipio_distrito_comuna_id, otra_referencia, paraje, provincia_id, codigo_postal) ";
		$ins_domicilio .= "VALUES ('0', "; 
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['EstabDatoCatastral'])."' ,"; /* datos_catastrales */ 
		$ins_domicilio .= $row_tierra['EstabDepartamento']." ,"; /* departamento_id */
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['EstabDomicilio'])."' ,"; /* direccion */
		 /* geo_lat, geo_lat_renaf, geo_long, geo_long_renaf, */ 
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['EstabLocalidad'])."' ,"; /* localidad */
		$ins_domicilio .= $row_tierra['EstabMunicipio']." ,"; /* municipio_distrito_comuna_id */ 
		/* nrookm */
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['EstabOtraReferencia'])."' ,"; /* otra_referencia */ 
		$ins_domicilio .= "'".mysqli_real_escape_string($Tconn, $row_tierra['EstabParaje'])."' ,"; /* paraje */
		$ins_domicilio .= $row_tierra['EstabProvincia']." ,"; /* provincia_id */ 
		$ins_domicilio .= "'".$row_tierra['EstabCP']."' "; /* codigo_postal */
		$ins_domicilio .= ")";
		echoif("Domicilio Produccion\n");
		echoif($ins_domicilio."\n");
		$id_domicilio_prod = 0;
		if (!$Tconn->query($ins_domicilio)) {
			pdberror($Tconn, $ins_domicilio."\n"."INSERT domicilio (produccion) failed: ");$Errores['domicilio']++;echoif("\n\n");
		} else {    
			$id_domicilio_prod = $Tconn->insert_id;
			echoif(" id domicilio Produccion:".$id_domicilio_prod."\n");
		}
				
		  /***************/
		 /* ACTIVIDADES */
		/***************/
		/* naf_completo 
		 * 	has 1
		 * 		actividad_completa 
		 * 		has many
		 * 			actividad_complementaria   rel   actividad_completa_actividad_complementaria
		 * 			actividad_principal   rel   actividad_completa_actividad_principal
		 * 			has many
		 * 				act_produccion 
		 * 				act_agricultura_detalle   rel   act_agricultura_act_agricultura_detalle
		 * 				act_agroindustria_detalle   rel   act_agroindustria_act_agroindustria_detalle
		 * 				act_apicultura_detalle   rel   act_apicultura_act_apicultura_detalle
		 * 				act_artesania_detalle   rel   act_artesania_act_artesania_detalle
		 * 				act_caza_detalle   rel   act_caza_act_caza_detalle
		 * 				act_pesca_detalle   rel   act_pesca_act_pesca_detalle
		 * 				act_recoleccion_detalle   rel   act_recoleccion_act_recoleccion_detalle
		 * 				act_turismo_rural_detalle   rel   act_turismo_rural_act_turismo_rural_detalle
		 * 				act_pastoreo_detalle   rel   act_pastoreo_act_pastoreo_detalle
		 * 				has many
		 * 					sub_producto_animal   rel   act_pastoreo_detalle_sub_producto_animal
		 *
		 * 7   actividad_principal ACT_PRINCIPAL_AGRICULTURA = 'actPrincipalAgricultura', 'renaf.ActAgricultura
		 * 8.a actividad_principal ACT_PRINCIPAL_PASTOREO = 'actPrincipalPastoreo', 'renaf.ActPastoreo'
		 * 8.d actividad_principal ACT_PRINCIPAL_APICULTURA = 'actPrincipalApicultura', 'renaf.ActApicultura'
		 * 9   actividad_principal ACT_PRINCIPAL_ARTESANIA = 'actPrincipalArtesania', 'renaf.ActArtesania'
		 * 10  actividad_principal ACT_PRINCIPAL_AGROINDUSTRIA = 'actPrincipalAgroindustria', 'renaf.ActAgroindustria'
		 * 12  actividad_principal ACT_PRINCIPAL_CAZA = 'actPrincipalCaza', 'renaf.ActCaza'
		 * 13  actividad_principal ACT_PRINCIPAL_PESCA = 'actPrincipalPesca', 'renaf.ActPesca'
		 * 11  actividad_principal ACT_PRINCIPAL_RECOLECCION = 'actPrincipalRecoleccion', 'renaf.ActRecoleccion'
		 * 14  actividad_principal ACT_PRINCIPAL_TURISMO_RURAL = 'actPrincipalTurismoRural', 'renaf.ActTurismoRural' 
		 */
		 
		/* actividad_completa */
		$ins_actividad_completa = " insert into actividad_completa (version, ingreso_adicional_anual, ingreso_complementario_anual) ";
		$ins_actividad_completa .= " values (0, ".$row_tit['IngBrOtros'].", ".$row_tit['IngBrServicios'].")";
		echoif("actividad_completa\n");
		echoif($ins_actividad_completa."\n");
		if (!$Tconn->query($ins_actividad_completa)) {
			pdberror($Tconn, $ins_actividad_completa."\n"."INSERT actividad_completa failed: ");$Errores['actividad_completa']++;echoif("\n\n");
		} else {    
			$id_actividad_completa = $Tconn->insert_id;
			echoif(" id actividad_completa:".$id_actividad_completa."\n");
		}		

		  /*******************************/		 
		 /* ACTIVIDADES COMPLEMENTARIAS */
		/*******************************/
		/* actividad_complementaria */
		/* -literales- 
		 * alquilaTierra
		 * trabajosFijos
		 * trabajosEventuales
		 * serviciosAnimales
		 * serviciosMaquinas
		 * vendeProductosOtros
		 * transporteProductosOtros
		 * transportaPersonas
		 */
		if ($row_tit['ServAlqTierra']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, 0, 'ServAlqTierra')";
			echoif("actividad_complementaria (alquilaTierra)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (alquilaTierra) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			
			/* superficie */
			$id_superficie = ins_superficie($Tconn, $row_tit['ServSuperficieAlquilada'], $row_tit['ServSuperficieAlquiladaUnidades']);
			
			$upd_actividad_complementaria = " update actividad_complementaria set superficie_id = ".$id_superficie;
			$upd_actividad_complementaria .= " where id = ".$id_actividad_complementaria;
			echoif("upd actividad_complementaria (superficie alquilaTierra)\n");
			echoif($upd_actividad_complementaria."\n");
			if (!$Tconn->query($upd_actividad_complementaria)) {
				pdberror($Tconn, $upd_actividad_complementaria."\n"."UPDATE actividad_complementaria (superficie alquilaTierra) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			}

			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (alquilaTierra)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (alquilaTierra) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}		
				 
		if ($row_tit['ServVtaFuerzaTrab']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, ".$row_tit['ServVtaFuerzaTrabAgropecuario'].", 'ServVtaFuerzaTrab')";
			echoif("actividad_complementaria (ServVtaFuerzaTrab)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServVtaFuerzaTrab) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServVtaFuerzaTrab)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServVtaFuerzaTrab) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}

		if ($row_tit['ServVtaFuerzaTrabEv']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, ".$row_tit['ServVtaFuerzaTrabEvAgropecuario'].", 'ServVtaFuerzaTrabEv')";
			echoif("actividad_complementaria (ServVtaFuerzaTrabEv)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServVtaFuerzaTrabEv) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServVtaFuerzaTrabEv)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServVtaFuerzaTrabEv) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}
		
		if ($row_tit['ServUsoANimales']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, ".$row_tit['ServUsoANimalesAgropecuario'].", 'ServUsoANimales')";
			echoif("actividad_complementaria (ServUsoANimales)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServUsoANimales) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServUsoANimales)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServUsoANimales) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}
		
		if ($row_tit['ServUsoMaquinarias']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, ".$row_tit['ServUsoMaquinariasAgropecuario'].", 'ServUsoMaquinarias')";
			echoif("actividad_complementaria (ServUsoMaquinarias)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServUsoMaquinarias) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServUsoMaquinarias)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServUsoMaquinarias) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}
		
		if ($row_tit['ServComercializacion']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, 0, 'ServComercializacion')";
			echoif("actividad_complementaria (ServComercializacion)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServComercializacion) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServComercializacion)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServComercializacion) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}
		
		if ($row_tit['ServTransporte']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, 0, 'ServTransporte')";
			echoif("actividad_complementaria (ServTransporte)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServTransporte) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServTransporte)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServTransporte) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}

		if ($row_tit['ServTurismo']) {
			$ins_actividad_complementaria = " insert into actividad_complementaria (version, agropecuarios, descripcion) ";
			$ins_actividad_complementaria .= " values (0, 0, 'ServTurismo')";
			echoif("actividad_complementaria (ServTurismo)\n");
			echoif($ins_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_complementaria."\n"."INSERT actividad_complementaria (ServTurismo) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			} else {    
				$id_actividad_complementaria = $Tconn->insert_id;
				echoif(" id actividad_complementaria:".$id_actividad_complementaria."\n");
			}
			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (ServTurismo)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_complementaria."\n"."INSERT actividad_completa_actividad_complementaria (ServTurismo) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
				echoif("\n\n");
			}
		}

		  /***************************/		 
		 /* ACTIVIDADES ADICIONALES */
		/***************************/

		/* actividad_completa_adicionales */
		/* FALTA! IngBrOtros */
		if ($row_tit['AsigPorHijo'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicAsignacionPorHijo')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		if ($row_tit['pensionJubilacion'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicPensionJubilacion')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		if ($row_tit['pensionNoContrib'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicPensionNoContributiva')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		if ($row_tit['pensionGraciable'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicPensionGraciable')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		if ($row_tit['planAsistencialEmpleo'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicPlanAsistencialEmpleo')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		if ($row_tit['seguroDesempleo'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicSeguroDesempleo')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		if ($row_tit['ingresoOtro'] > 0) {		 
			$ins_actividad_completa_adicionales = " insert into actividad_completa_adicionales (actividad_completa_id, adicionales_string) ";
			$ins_actividad_completa_adicionales .= " values (".$id_actividad_completa.", 'ingAdicOtro')";
			echoif("actividad_completa_adicionales\n");
			echoif($ins_actividad_completa_adicionales."\n");
			if (!$Tconn->query($ins_actividad_completa_adicionales)) {
				pdberror($Tconn, $ins_actividad_completa_adicionales."\n"."INSERT actividad_completa_adicionales failed: ");
				$Errores['actividad_completa_adicionales']++;
				echoif("\n\n");
			}
		}
		  /* * * * * * * * * * * * * * * * * * * * * * * * * * */
		 /*  A C T I V I D A D E S     P R I N C I P A L E S  */
		/* * * * * * * * * * * * * * * * * * * * * * * * * * */
		/****************************************************************************************************/		
		/* actividad_principal ACT_PRINCIPAL_AGRICULTURA = 'actPrincipalAgricultura', 'renaf.ActAgricultura */
		/****************************************************************************************************/
		if ($row_tit['ProdVegetal']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalAgricultura', 'renaf.ActAgricultura', null, null, ".$row_tit['ProdVegetalOrganicaCertificada'].", ".$row_tit['ProdVegetalOrganica'].")";
			echoif("actividad_principal actPrincipalAgricultura\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalAgricultura failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalAgricultura)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalAgricultura failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_agricultura_detalle   rel   act_agricultura_act_agricultura_detalle */
			if ($nRows_pv > 0) {
				$pv=0;
				while ($row_pv = mysqli_fetch_array($res_pv)) {
					$pv++;
					
					/* act_produccion */
					$ins_act_produccion = " /* act_agricultura_detalle */ insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
					$ins_act_produccion .= " values (0, '".trim($row_pv['can_cod'])."', ".$row_pv['PVAutoconsumo'].", ".$row_pv['PVMercado'].", ".$row_pv['PVIntercambio'].", ".$row_pv['PVPrecioUnitario'].", ".$row_pv['PVVolumen'].", ".$row_pv['PVUnidad'].", '".$row_pv['exp_cod']."' )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion agricultura failed: ");$Errores['act_produccion']++;echoif("\n\n");
					} else {    
						$id_act_produccion = $Tconn->insert_id;
						echoif(" id act_produccion :".$id_act_produccion."\n");
					}
					/* superficie_completa */
					$id_superficie = ins_superficie($Tconn, $row_pv['PVACampo'], $row_pv['PVUn']);
					/* act_agricultura_detalle */
					$ins_act_agricultura_detalle = " insert into act_agricultura_detalle (version, bajo_cubierta, codigo, descripcion, produccion_id, superficie_id) ";
					$ins_act_agricultura_detalle .= " values (0, ".$row_pv['PVCubierta'].", '".$row_pv['act_cod']."', '".$row_pv['act_desc']."', ".$id_act_produccion.", ".$id_superficie.")";
					echoif("act_agricultura_detalle\n");
					echoif($ins_act_agricultura_detalle."\n");
					if (!$Tconn->query($ins_act_agricultura_detalle)) {
						pdberror($Tconn, $ins_act_agricultura_detalle."\n"."INSERT act_agricultura_detalle failed: ");
						$Errores['act_agricultura_detalle']++;
						echoif("\n\n");
					} else {    
						$id_act_agricultura_detalle = $Tconn->insert_id;
						echoif(" id act_agricultura_detalle :".$id_act_agricultura_detalle."\n");
					}
					/* act_agricultura_act_agricultura_detalle */
					$ins_act_agricultura_act_agricultura_detalle = " insert into act_agricultura_act_agricultura_detalle (act_agricultura_detalles_id, act_agricultura_detalle_id) ";
					$ins_act_agricultura_act_agricultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agricultura_detalle." )";
					echoif("act_agricultura_act_agricultura_detalle\n");
					echoif($ins_act_agricultura_act_agricultura_detalle."\n");
					if (!$Tconn->query($ins_act_agricultura_act_agricultura_detalle)) {
						pdberror($Tconn, $ins_act_agricultura_act_agricultura_detalle."\n"."INSERT act_agricultura_act_agricultura_detalle failed: ");
						$Errores['act_agricultura_act_agricultura_detalle']++;
						echoif("\n\n");
					}
				}
			}
		}

		/***********************************************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_AGROINDUSTRIA = 'actPrincipalAgroindustria', 'renaf.ActAgroindustria' */
		/***********************************************************************************************************/
		if ($row_tit['ProdAgroindustria']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalAgroindustria', 'renaf.ActAgroindustria', null, null, ".$row_tit['ProdAgrindustriaOrganicaCertificada'].", ".$row_tit['ProdAgrindustriaOrganica'].")";
			echoif("actividad_principal actPrincipalAgroindustria\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalAgroindustria failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalAgroindustria)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalAgroindustria failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_agroindustria_detalle   rel   act_agroindustria_act_agroindustria_detalle */
			if ($nRows_ai > 0) {
				$ai=0;
				while ($row_ai = mysqli_fetch_array($res_ai)) {
					$ai++;
					
					/* act_produccion */
					$ins_act_produccion = " /* act_agroindustria_detalle */ insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
					$ins_act_produccion .= " values (0, '".trim($row_ai['can_cod'])."', ".$row_ai['AgroindustriaAutoconsumo'].", ".$row_ai['AgroindustriaMercado'].", ".$row_ai['AgroindustriaIntercambio'].", ".$row_ai['AgroindustriaPrecio'].", ".$row_ai['AgroindustriaVolumen'].", ".$row_ai['AgroindustriaUnidad'].", '".$row_ai['AgroindustriaExplotacion']."' )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion agroindustria failed: ");$Errores['act_produccion']++;echoif("\n\n");
					} else {    
						$id_act_produccion = $Tconn->insert_id;
						echoif(" id act_produccion :".$id_act_produccion."\n");
					}
					/* act_agroindustria_detalle */
					$ins_act_agroindustria_detalle = " insert into act_agroindustria_detalle (version, codigo, descripcion, produccion_id, produce_materia_prima) ";
					$ins_act_agroindustria_detalle .= " values (0, '".$row_ai['act_cod']."', '".$row_ai['act_desc']."', ".$id_act_produccion.", ".$row_ai['AgroindustriaProduceMP'].")";
					echoif("act_agroindustria_detalle\n");
					echoif($ins_act_agroindustria_detalle."\n");
					if (!$Tconn->query($ins_act_agroindustria_detalle)) {
						pdberror($Tconn, $ins_act_agroindustria_detalle."\n"."INSERT act_agroindustria_detalle failed: ");
						$Errores['act_agroindustria_detalle']++;
						echoif("\n\n");
					} else {    
						$id_act_agroindustria_detalle = $Tconn->insert_id;
						echoif(" id act_agroindustria_detalle :".$id_act_agroindustria_detalle."\n");
					}
					/* act_agroindustria_act_agroindustria_detalle */
					$ins_act_agroindustria_act_agroindustria_detalle = " insert into act_agroindustria_act_agroindustria_detalle (act_agroindustria_detalles_id, act_agroindustria_detalle_id) ";
					$ins_act_agroindustria_act_agroindustria_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
					echoif("act_agroindustria_act_agroindustria_detalle\n");
					echoif($ins_act_agroindustria_act_agroindustria_detalle."\n");
					if (!$Tconn->query($ins_act_agroindustria_act_agroindustria_detalle)) {
						pdberror($Tconn, $ins_act_agroindustria_act_agroindustria_detalle."\n"."INSERT act_agroindustria_act_agroindustria_detalle failed: ");
						$Errores['act_agroindustria_act_agroindustria_detalle']++;
						echoif("\n\n");
					}
				}
			}
		}			

		/**************************************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_APICULTURA = 'actPrincipalApicultura', 'renaf.ActApicultura' */
		/**************************************************************************************************/
		if ($row_tit['PAApiculturaColmenasPropias'] + $row_tit['PAApiculturaColmenasTerceros'] > 0) {					
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalApicultura', 'renaf.ActApicultura', ".$row_tit['PAApiculturaColmenasPropias'].", ".$row_tit['PAApiculturaColmenasTerceros'].", 0, 0)";
			echoif("actividad_principal actPrincipalApicultura\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalApicultura failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalApicultura)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalApicultura failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_apicultura_detalle   rel   act_apicultura_act_apicultura_detalle */
			/* MIEL */
			/* act_produccion */
			$ins_act_produccion = " /* act_apicultura_detalle */ insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaMielCanal'])."', ".$row_tit['PAApiculturaMielAutoConsumo'].", ".$row_tit['PAApiculturaMielMercado'].", ".$row_tit['PAApiculturaMielIntercambio'].", ".$row_tit['PAApiculturaMielPrecioUnitario'].", ".$row_tit['PAApiculturaMielVolumen'].", ".$row_tit['PAApiculturaMielUnidades'].", '".$row_tit['PAApiculturaMielExplotacion']."' )";
			echoif("act_produccion MIEL\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion MIEL failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaMielColmenas']."', '201', 'MIEL', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle MIEL:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* CERA */
			/* act_produccion */
			$ins_act_produccion = " /* CERA */ insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaCeraCanal'])."', ".$row_tit['PAApiculturaCeraAutoConsumo'].", ".$row_tit['PAApiculturaCeraMercado'].", ".$row_tit['PAApiculturaCeraIntercambio'].", ".$row_tit['PAApiculturaCeraPrecioUnitario'].", ".$row_tit['PAApiculturaCeraVolumen'].", ".$row_tit['PAApiculturaCeraUnidades'].", '".$row_tit['PAApiculturaCeraExplotacion']."' )";
			echoif("act_produccion CERA\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion CERA failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaCeraColmenas']."', '202', 'CERA', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle CERA:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* PROPOLEO */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaPropoleoCanal'])."', ".$row_tit['PAApiculturaPropoleoAutoConsumo'].", ".$row_tit['PAApiculturaPropoleoMercado'].", ".$row_tit['PAApiculturaPropoleoIntercambio'].", ".$row_tit['PAApiculturaPropoleoPrecioUnitario'].", ".$row_tit['PAApiculturaPropoleoVolumen'].", ".$row_tit['PAApiculturaPropoleoUnidades'].", '".$row_tit['PAApiculturaPropoleoExplotacion']."' )";
			echoif("act_produccion PROPOLEO\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion PROPOLEO failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaPropoleoColmenas']."', '203', 'PROPOLEOS', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle PROPOLEO:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* JALEA */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaJaleaCanal'])."', ".$row_tit['PAApiculturaJaleaAutoConsumo'].", ".$row_tit['PAApiculturaJaleaMercado'].", ".$row_tit['PAApiculturaJaleaIntercambio'].", ".$row_tit['PAApiculturaJaleaPrecioUnitario'].", ".$row_tit['PAApiculturaJaleaVolumen'].", ".$row_tit['PAApiculturaJaleaUnidades'].", '".$row_tit['PAApiculturaJaleaExplotacion']."' )";
			echoif("act_produccion JALEA\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion JALEA failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaJaleaColmenas']."', '204', 'JALEA', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle JALEA:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* POLEN */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaPolenCanal'])."', ".$row_tit['PAApiculturaPolenAutoConsumo'].", ".$row_tit['PAApiculturaPolenMercado'].", ".$row_tit['PAApiculturaPolenIntercambio'].", ".$row_tit['PAApiculturaPolenPrecioUnitario'].", ".$row_tit['PAApiculturaPolenVolumen'].", ".$row_tit['PAApiculturaPolenUnidades'].", '".$row_tit['PAApiculturaPolenExplotacion']."' )";
			echoif("act_produccion POLEN\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion POLEN failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaPolenColmenas']."', '205', 'POLEN', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle POLEN:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* NUCLEOS */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaNucleosCanal'])."', ".$row_tit['PAApiculturaNucleosAutoConsumo'].", ".$row_tit['PAApiculturaNucleosMercado'].", ".$row_tit['PAApiculturaNucleosIntercambio'].", ".$row_tit['PAApiculturaNucleosPrecioUnitario'].", ".$row_tit['PAApiculturaNucleosVolumen'].", ".$row_tit['PAApiculturaNucleosUnidades'].", '".$row_tit['PAApiculturaNucleosExplotacion']."' )";
			echoif("act_produccion NUCLEOS\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion NUCLEOS failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaNucleosColmenas']."', '206', 'NUCLEOS', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle NUCLEOS:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* REINAS */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaMaterialVicoCanal'])."', ".$row_tit['PAApiculturaMaterialVivoAutoConsumo'].", ".$row_tit['PAApiculturaMaterialVivoMercado'].", ".$row_tit['PAApiculturaMaterialVivoIntercambio'].", ".$row_tit['PAApiculturaMaterialVivoPrecioUnitario'].", ".$row_tit['PAApiculturaMaterialVivoVolumen'].", ".$row_tit['PAApiculturaMaterialVivoUnidades'].", '".$row_tit['PAApiculturaMaterialVivoExplotacion']."' )";
			echoif("act_produccion REINAS\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion REINAS failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_apicultura_detalle */
			$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
			$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaMaterialVivoColmenas']."', '207', 'REINAS', ".$id_act_produccion.")";
			echoif("act_apicultura_detalle\n");
			echoif($ins_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_detalle)) {
				pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle REINAS:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* OTRO */
			if (!empty($row_tit['PAApiculturaOtroAutoConsumo']) &&
				 empty($row_tit['PAApiculturaOtroUnidades']) &&
				 empty($row_tit['PAApiculturaOtroVolumen']) ) {
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
				$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaOtroCanal'])."', ".$row_tit['PAApiculturaOtroAutoConsumo'].", ".$row_tit['PAApiculturaOtroMercado'].", ".$row_tit['PAApiculturaOtroIntercambio'].", ".$row_tit['PAApiculturaOtroPrecioUnitario'].", ".$row_tit['PAApiculturaOtroVolumen'].", ".$row_tit['PAApiculturaOtroUnidades'].", '".$row_tit['PAApiculturaOtroExplotacion']."' )";
				echoif("act_produccion OTRO\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion OTRO failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_apicultura_detalle */
				$ins_act_apicultura_detalle = " insert into act_apicultura_detalle (version, cantidad_de_colmenas, codigo, descripcion, produccion_id) ";
				$ins_act_apicultura_detalle .= " values (0, '".$row_tit['PAApiculturaOtroColmenas']."', '208', 'OTRO', ".$id_act_produccion.")";
				echoif("act_apicultura_detalle\n");
				echoif($ins_act_apicultura_detalle."\n");
				if (!$Tconn->query($ins_act_apicultura_detalle)) {
					pdberror($Tconn, $ins_act_apicultura_detalle."\n"."INSERT act_apicultura_detalle failed: ");
					$Errores['act_apicultura_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_apicultura_detalle = $Tconn->insert_id;
					echoif(" id act_apicultura_detalle OTRO:".$id_act_apicultura_detalle."\n");
				}
				/* act_apicultura_act_apicultura_detalle */
				$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
				$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_apicultura_detalle." )";
				echoif("act_apicultura_act_apicultura_detalle\n");
				echoif($ins_act_apicultura_act_apicultura_detalle."\n");
				if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
					pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
					$Errores['act_apicultura_act_apicultura_detalle']++;
					echoif("\n\n");
				}
			}
		}

		/***********************************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_ARTESANIA = 'actPrincipalArtesania', 'renaf.ActArtesania' */
		/***********************************************************************************************/
		if ($row_tit['ProdArtesania']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalArtesania', 'renaf.ActArtesania', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalArtesania\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalArtesania failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalArtesania)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalArtesania failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_artesania_detalle   rel   act_artesania_act_artesania_detalle */
			if ($nRows_ar > 0) {
				$ar=0;
				while ($row_ar = mysqli_fetch_array($res_ar)) {
					$ar++;
					
					/* act_produccion */
					$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
					$ins_act_produccion .= " values (0, '".trim($row_ar['can_cod'])."', 0, 0, 0, ".$row_ar['ArtesaniaPrecio'].", ".$row_ar['ArtesaniaVolumen'].", ".$row_ar['ArtesaniaUnidad'].", '".$row_ar['ArtesaniaExplotacion']."' )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion failed: ");$Errores['act_produccion']++;echoif("\n\n");
					} else {    
						$id_act_produccion = $Tconn->insert_id;
						echoif(" id act_produccion :".$id_act_produccion."\n");
					}
					/* act_artesania_detalle */
					$ins_act_artesania_detalle = " insert into act_artesania_detalle (version, codigo, descripcion, produccion_id, produce_materia_prima) ";
					$ins_act_artesania_detalle .= " values (0, '".$row_ar['act_cod']."', '".$row_ar['act_desc']."', ".$id_act_produccion.", ".$row_ar['ArtesaniaCosechaMP'].")";
					echoif("act_artesania_detalle\n");
					echoif($ins_act_artesania_detalle."\n");
					if (!$Tconn->query($ins_act_artesania_detalle)) {
						pdberror($Tconn, "INSERT act_artesania_detalle failed: ");
						$Errores['act_artesania_detalle']++;
						echoif("\n\n");
					} else {    
						$id_act_artesania_detalle = $Tconn->insert_id;
						echoif(" id act_artesania_detalle :".$id_act_artesania_detalle."\n");
					}
					/* act_artesania_act_artesania_detalle */
					$ins_act_artesania_act_artesania_detalle = " insert into act_artesania_act_artesania_detalle (act_artesania_detalles_id, act_artesania_detalle_id) ";
					$ins_act_artesania_act_artesania_detalle .= " values (".$id_actividad_principal.", ".$id_act_artesania_detalle." )";
					echoif("act_artesania_act_artesania_detalle\n");
					echoif($ins_act_artesania_act_artesania_detalle."\n");
					if (!$Tconn->query($ins_act_artesania_act_artesania_detalle)) {
						pdberror($Tconn, "INSERT act_artesania_act_artesania_detalle failed: ");
						$Errores['act_artesania_act_artesania_detalle']++;
						echoif("\n\n");
					}
				}
			}
		}

		/********************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_CAZA = 'actPrincipalCaza', 'renaf.ActCaza' */
		/********************************************************************************/
		if ($row_tit['ProdCaza']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalCaza', 'renaf.ActCaza', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalCaza\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalCaza failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalCaza)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalCaza failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_caza_detalle   rel   act_caza_act_caza_detalle */
			/* CARNE */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PRodCazaCarneCanal'])."', ".$row_tit['PRodCazaCarneAutoConsumo'].", ".$row_tit['PRodCazaCarneMercado'].", ".$row_tit['PRodCazaCarneIntercambio'].", ".$row_tit['PRodCazaCarnePrecio'].", ".$row_tit['PRodCazaCarneVolumen'].", ".$row_tit['PRodCazaCarneUnidad'].", '".$row_tit['PRodCazaCarneExplotacion']."' )";
			echoif("act_produccion CARNE\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion CARNE failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_caza_detalle */ 
			$ins_act_caza_detalle = " insert into act_caza_detalle (version, codigo, descripcion, produccion_id) ";
			$ins_act_caza_detalle .= " values (0,  '301', 'CARNE', ".$id_act_produccion.")";
			echoif("act_caza_detalle\n");
			echoif($ins_act_caza_detalle."\n");
			if (!$Tconn->query($ins_act_caza_detalle)) {
				pdberror($Tconn, "INSERT act_caza_detalle CARNE failed: ");
				$Errores['act_caza_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_caza_detalle = $Tconn->insert_id;
				echoif(" id act_caza_detalle CARNE:".$id_act_caza_detalle."\n");
			}
			/* act_caza_act_caza_detalle */
			$ins_act_caza_act_caza_detalle = " insert into act_caza_act_caza_detalle (act_caza_detalles_id, act_caza_detalle_id) ";
			$ins_act_caza_act_caza_detalle .= " values (".$id_actividad_principal.", ".$id_act_caza_detalle." )";
			echoif("act_caza_act_caza_detalle\n");
			echoif($ins_act_caza_act_caza_detalle."\n");
			if (!$Tconn->query($ins_act_caza_act_caza_detalle)) {
				pdberror($Tconn, "INSERT act_caza_act_caza_detalle CARNE failed: ");
				$Errores['act_caza_act_caza_detalle']++;
				echoif("\n\n");
			}
			/* CUERO/PIELES */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PRodCazaCuerosCanal'])."', ".$row_tit['PRodCazaCuerosAutoConsumo'].", ".$row_tit['PRodCazaCuerosMercado'].", ".$row_tit['PRodCazaCuerosIntercambio'].", ".$row_tit['PRodCazaCuerosPrecio'].", ".$row_tit['PRodCazaCuerosVolumen'].", ".$row_tit['PRodCazaCuerosUnidad'].", '".$row_tit['PRodCazaCuerosExplotacion']."' )";
			echoif("act_produccion CUERO/PIELES\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion CUERO/PIELES failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_caza_detalle */ 
			$ins_act_caza_detalle = " insert into act_caza_detalle (version, codigo, descripcion, produccion_id) ";
			$ins_act_caza_detalle .= " values (0,  '302', 'CUERO/PIELES', ".$id_act_produccion.")";
			echoif("act_caza_detalle\n");
			echoif($ins_act_caza_detalle."\n");
			if (!$Tconn->query($ins_act_caza_detalle)) {
				pdberror($Tconn, "INSERT act_caza_detalle CUERO/PIELES failed: ");
				$Errores['act_caza_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_caza_detalle = $Tconn->insert_id;
				echoif(" id act_caza_detalle CUERO/PIELES:".$id_act_caza_detalle."\n");
			}
			/* act_caza_act_caza_detalle */
			$ins_act_caza_act_caza_detalle = " insert into act_caza_act_caza_detalle (act_caza_detalles_id, act_caza_detalle_id) ";
			$ins_act_caza_act_caza_detalle .= " values (".$id_actividad_principal.", ".$id_act_caza_detalle." )";
			echoif("act_caza_act_caza_detalle\n");
			echoif($ins_act_caza_act_caza_detalle."\n");
			if (!$Tconn->query($ins_act_caza_act_caza_detalle)) {
				pdberror($Tconn, "INSERT act_caza_act_caza_detalle CUERO/PIELES failed: ");
				$Errores['act_caza_act_caza_detalle']++;
				echoif("\n\n");
			}
			/* OTRO */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PRodCazaOtrosCanal'])."', ".$row_tit['PRodCazaOtrosAutoConsumo'].", ".$row_tit['PRodCazaOtrosMercado'].", ".$row_tit['PRodCazaOtrosIntercambio'].", ".$row_tit['PRodCazaOtrosPrecio'].", ".$row_tit['PRodCazaOtrosVolumen'].", ".$row_tit['PRodCazaOtrosUnidad'].", '".$row_tit['PRodCazaOtrosExplotacion']."' )";
			echoif("act_produccion OTRO\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion OTRO failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_caza_detalle */ 
			$ins_act_caza_detalle = " insert into act_caza_detalle (version, codigo, descripcion, produccion_id) ";
			$ins_act_caza_detalle .= " values (0,  '303', 'OTRO', ".$id_act_produccion.")";
			echoif("act_caza_detalle\n");
			echoif($ins_act_caza_detalle."\n");
			if (!$Tconn->query($ins_act_caza_detalle)) {
				pdberror($Tconn, "INSERT act_caza_detalle OTRO failed: ");
				$Errores['act_caza_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_caza_detalle = $Tconn->insert_id;
				echoif(" id act_caza_detalle OTRO:".$id_act_caza_detalle."\n");
			}
			/* act_caza_act_caza_detalle */
			$ins_act_caza_act_caza_detalle = " insert into act_caza_act_caza_detalle (act_caza_detalles_id, act_caza_detalle_id) ";
			$ins_act_caza_act_caza_detalle .= " values (".$id_actividad_principal.", ".$id_act_caza_detalle." )";
			echoif("act_caza_act_caza_detalle\n");
			echoif($ins_act_caza_act_caza_detalle."\n");
			if (!$Tconn->query($ins_act_caza_act_caza_detalle)) {
				pdberror($Tconn, "INSERT act_caza_act_caza_detalle OTRO failed: ");
				$Errores['act_caza_act_caza_detalle']++;
				echoif("\n\n");
			}
		}

		/***********************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_PESCA = 'actPrincipalPesca', 'renaf.ActPesca' */
		/***********************************************************************************/
		if ($row_tit['ProdPesca']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalPesca', 'renaf.ActPesca', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalPesca\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalPesca failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalPesca)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalPesca failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_pesca_detalle   rel   act_pesca_act_pesca_detalle */
			/* CRIADERO */
			if (! empty($row_tit['PAPescaCriaderoVolumen']) && 
				  empty($row_tit['PAPescaCriaderoUnidad']) &&
				  empty($row_tit['PAPescaCriaderoAutoConsumo']) ) {
						
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
				$ins_act_produccion .= " values (0, '".trim($row_tit['PAPescaCriaderoCanal'])."', ".$row_tit['PAPescaCriaderoAutoConsumo'].", ".$row_tit['PAPescaCriaderoMercado'].", ".$row_tit['PAPescaCriaderoIntercambio'].", ".$row_tit['PAPescaCriaderoPrecio'].", ".$row_tit['PAPescaCriaderoVolumen'].", ".$row_tit['PAPescaCriaderoUnidad'].", '".$row_tit['PAPescaCriaderoExplotacion']."' )";
				echoif("act_produccion CRIADERO\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion CRIADERO failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_pesca_detalle */ 
				$ins_act_pesca_detalle = " insert into act_pesca_detalle (version, codigo, descripcion, produccion_id, sistema, tipo) ";
				$ins_act_pesca_detalle .= " values (0,  '101', 'CRIADERO', ".$id_act_produccion.", 'CRIADERO', ".$row_tit['PAPescaCriaderoTipo'].")";
				echoif("act_pesca_detalle\n");
				echoif($ins_act_pesca_detalle."\n");
				if (!$Tconn->query($ins_act_pesca_detalle)) {
					pdberror($Tconn, "INSERT act_pesca_detalle CRIADERO failed: ");
					$Errores['act_pesca_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_pesca_detalle = $Tconn->insert_id;
					echoif(" id act_pesca_detalle CRIADERO:".$id_act_pesca_detalle."\n");
				}
				/* act_pesca_act_pesca_detalle */
				$ins_act_pesca_act_pesca_detalle = " insert into act_pesca_act_pesca_detalle (act_pesca_detalles_id, act_pesca_detalle_id) ";
				$ins_act_pesca_act_pesca_detalle .= " values (".$id_actividad_principal.", ".$id_act_pesca_detalle." )";
				echoif("act_pesca_act_pesca_detalle\n");
				echoif($ins_act_pesca_act_pesca_detalle."\n");
				if (!$Tconn->query($ins_act_pesca_act_pesca_detalle)) {
					pdberror($Tconn, "INSERT act_pesca_act_pesca_detalle CRIADERO failed: ");
					$Errores['act_pesca_act_pesca_detalle']++;
					echoif("\n\n");
				}
			}
			/* RECOLECCION */
			if (! empty($row_tit['PAPescaRecoleccionVolumen']) && 
				  empty($row_tit['PAPescaRecoleccionUnidad']) &&
				  empty($row_tit['PAPescaRecoleccionAutoConsumo']) ) {
						/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PAPescaRecoleccionCanal'])."', ".$row_tit['PAPescaRecoleccionAutoConsumo'].", ".$row_tit['PAPescaRecoleccionMercado'].", ".$row_tit['PAPescaRecoleccionIntercambio'] . ", " . $row_tit['PAPescaRecoleccionPrecio'] . ", ".$row_tit['PAPescaRecoleccionVolumen'] . ", ".$row_tit['PAPescaRecoleccionUnidad'].", '". $row_tit['PAPescaRecoleccionExplotacion']."' )"; 
				echoif("act_produccion RECOLECCION\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion RECOLECCION failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_pesca_detalle */ 
				$ins_act_pesca_detalle = " insert into act_pesca_detalle (version, codigo, descripcion, produccion_id, sistema, tipo) ";
				$ins_act_pesca_detalle .= " values (0,  '102', 'RECOLECCION', ".$id_act_produccion.", 'RECOLECCION', ".$row_tit['PAPescaRecoleccionTipo'].")";
				echoif("act_pesca_detalle\n");
				echoif($ins_act_pesca_detalle."\n");
				if (!$Tconn->query($ins_act_pesca_detalle)) {
					pdberror($Tconn, "INSERT act_pesca_detalle RECOLECCION failed: ");
					$Errores['act_pesca_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_pesca_detalle = $Tconn->insert_id;
					echoif(" id act_pesca_detalle RECOLECCION:".$id_act_pesca_detalle."\n");
				}
				/* act_pesca_act_pesca_detalle */
				$ins_act_pesca_act_pesca_detalle = " insert into act_pesca_act_pesca_detalle (act_pesca_detalles_id, act_pesca_detalle_id) ";
				$ins_act_pesca_act_pesca_detalle .= " values (".$id_actividad_principal.", ".$id_act_pesca_detalle." )";
				echoif("act_pesca_act_pesca_detalle\n");
				echoif($ins_act_pesca_act_pesca_detalle."\n");
				if (!$Tconn->query($ins_act_pesca_act_pesca_detalle)) {
					pdberror($Tconn, "INSERT act_pesca_act_pesca_detalle RECOLECCION failed: ");
					$Errores['act_pesca_act_pesca_detalle']++;
					echoif("\n\n");
				}
			}
			/* CAPTURA */
			if (! empty($row_tit['PAPescaCapturaVolumen']) && 
				  empty($row_tit['PAPescaCapturaUnidad']) &&
				  empty($row_tit['PAPescaCapturaAutoConsumo']) ) {
						
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PAPescaCapturaCanal'])."', ".$row_tit['PAPescaCapturaAutoConsumo'].", ".$row_tit['PAPescaCapturaMercado'].", ".$row_tit['PAPescaCapturaIntercambio'] . ", " . $row_tit['PAPescaCapturaPrecio'] . ", ".$row_tit['PAPescaCapturaVolumen'] . ", ".$row_tit['PAPescaCapturaUnidad'].", '". $row_tit['PAPescaCapturaExplotacion']."' )"; 
				echoif("act_produccion CAPTURA\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion CAPTURA failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_pesca_detalle */ 
				$ins_act_pesca_detalle = " insert into act_pesca_detalle (version, codigo, descripcion, produccion_id, sistema, tipo) ";
				$ins_act_pesca_detalle .= " values (0,  '103', 'CAPTURA', ".$id_act_produccion.", 'CAPTURA', ".$row_tit['PAPescaCapturaTipo'].")";
				echoif("act_pesca_detalle\n");
				echoif($ins_act_pesca_detalle."\n");
				if (!$Tconn->query($ins_act_pesca_detalle)) {
					pdberror($Tconn, "INSERT act_pesca_detalle CAPTURA failed: ");
					$Errores['act_pesca_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_pesca_detalle = $Tconn->insert_id;
					echoif(" id act_pesca_detalle CAPTURA:".$id_act_pesca_detalle."\n");
				}
				/* act_pesca_act_pesca_detalle */
				$ins_act_pesca_act_pesca_detalle = " insert into act_pesca_act_pesca_detalle (act_pesca_detalles_id, act_pesca_detalle_id) ";
				$ins_act_pesca_act_pesca_detalle .= " values (".$id_actividad_principal.", ".$id_act_pesca_detalle." )";
				echoif("act_pesca_act_pesca_detalle\n");
				echoif($ins_act_pesca_act_pesca_detalle."\n");
				if (!$Tconn->query($ins_act_pesca_act_pesca_detalle)) {
					pdberror($Tconn, "INSERT act_pesca_act_pesca_detalle CAPTURA failed: ");
					$Errores['act_pesca_act_pesca_detalle']++;
					echoif("\n\n");
				}
			}
		}

		/*****************************************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_RECOLECCION = 'actPrincipalRecoleccion', 'renaf.ActRecoleccion' */
		/*****************************************************************************************************/
		if ($row_tit['ProdRecoleccion']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalRecoleccion', 'renaf.ActRecoleccion', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalRecoleccion\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalRecoleccion failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalRecoleccion)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalRecoleccion failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_recoleccion_detalle   rel   act_recoleccion_act_recoleccion_detalle */
			/* VEGETALES */
			if (! empty($row_tit['PRodRecoleccionVegetalesVolumen']) && 
				  empty($row_tit['PRodRecoleccionVegetalesUnidad']) &&
				  empty($row_tit['PRodRecoleccionVegetalesAutoConsumo']) ) {
						
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PRodRecoleccionVegetalCanal'])."', ".$row_tit['PRodRecoleccionVegetalesAutoConsumo'].", ".$row_tit['PRodRecoleccionVegetalesMercado'].", ".$row_tit['PRodRecoleccionVegetalesIntercambio'] . ", " . $row_tit['PRodRecoleccionVegetalesPrecio'] . ", ".$row_tit['PRodRecoleccionVegetalesVolumen'] . ", ".$row_tit['PRodRecoleccionVegetalesUnidad'].", '". $row_tit['PRodRecoleccionVegetalesExplotacion']."' )"; 
				echoif("act_produccion VEGETALES\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion VEGETALES failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_recoleccion_detalle */ 
				$ins_act_recoleccion_detalle = " insert into act_recoleccion_detalle (version, codigo, descripcion, produccion_id) ";
				$ins_act_recoleccion_detalle .= " values (0,  '401', 'VEGETAL', ".$id_act_produccion.")";
				echoif("act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_detalle VEGETALES failed: ");
					$Errores['act_recoleccion_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_recoleccion_detalle = $Tconn->insert_id;
					echoif(" id act_recoleccion_detalle VEGETALES:".$id_act_recoleccion_detalle."\n");
				}
				/* act_recoleccion_act_recoleccion_detalle */
				$ins_act_recoleccion_act_recoleccion_detalle = " insert into act_recoleccion_act_recoleccion_detalle (act_recoleccion_detalles_id, act_recoleccion_detalle_id) ";
				$ins_act_recoleccion_act_recoleccion_detalle .= " values (".$id_actividad_principal.", ".$id_act_recoleccion_detalle." )";
				echoif("act_recoleccion_act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_act_recoleccion_detalle VEGETALES failed: ");
					$Errores['act_recoleccion_act_recoleccion_detalle']++;
					echoif("\n\n");
				}
			}
			/* MINERALES */
			if (! empty($row_tit['PRodRecoleccionMineralesVolumen']) && 
				  empty($row_tit['PRodRecoleccionMineralesUnidad']) &&
				  empty($row_tit['PRodRecoleccionMineralesAutoConsumo']) ) {
						
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PRodRecoleccionMineralCanal'])."', ".$row_tit['PRodRecoleccionMineralesAutoConsumo'].", ".$row_tit['PRodRecoleccionMineralesMercado'].", ".$row_tit['PRodRecoleccionMineralesIntercambio'] . ", " . $row_tit['PRodRecoleccionMineralesPrecio'] . ", ".$row_tit['PRodRecoleccionMineralesVolumen'] . ", ".$row_tit['PRodRecoleccionMineralesUnidad'].", '". $row_tit['PRodRecoleccionMineralesExplotacion']."' )";
				echoif("act_produccion MINERALES\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion MINERALES failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_recoleccion_detalle */ 
				$ins_act_recoleccion_detalle = " insert into act_recoleccion_detalle (version, codigo, descripcion, produccion_id) ";
				$ins_act_recoleccion_detalle .= " values (0,  '402', 'MINERAL', ".$id_act_produccion.")";
				echoif("act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_detalle MINERALES failed: ");
					$Errores['act_recoleccion_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_recoleccion_detalle = $Tconn->insert_id;
					echoif(" id act_recoleccion_detalle MINERALES:".$id_act_recoleccion_detalle."\n");
				}
				/* act_recoleccion_act_recoleccion_detalle */
				$ins_act_recoleccion_act_recoleccion_detalle = " insert into act_recoleccion_act_recoleccion_detalle (act_recoleccion_detalles_id, act_recoleccion_detalle_id) ";
				$ins_act_recoleccion_act_recoleccion_detalle .= " values (".$id_actividad_principal.", ".$id_act_recoleccion_detalle." )";
				echoif("act_recoleccion_act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_act_recoleccion_detalle MINERALES failed: ");
					$Errores['act_recoleccion_act_recoleccion_detalle']++;
					echoif("\n\n");
				}
			}
			/* HONGOS */
			/* act_produccion */
			if (! empty($row_tit['PRodRecoleccionHongosVolumen']) && 
				  empty($row_tit['PRodRecoleccionHongosUnidad']) &&
				  empty($row_tit['PRodRecoleccionHongosAutoConsumo']) ) {
			
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PRodRecoleccionHongosCanal'])."', ".$row_tit['PRodRecoleccionHongosAutoConsumo'].", ".$row_tit['PRodRecoleccionHongosMercado'].", ".$row_tit['PRodRecoleccionHongosIntercambio'].", ".$row_tit['PRodRecoleccionHongosPrecio'] . ", ".$row_tit['PRodRecoleccionHongosVolumen'] . ", ".$row_tit['PRodRecoleccionHongosUnidad'].", '". $row_tit['PRodRecoleccionHongosExplotacion']."' )";
				echoif("act_produccion HONGOS\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion HONGOS failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_recoleccion_detalle */ 
				$ins_act_recoleccion_detalle = " insert into act_recoleccion_detalle (version, codigo, descripcion, produccion_id) ";
				$ins_act_recoleccion_detalle .= " values (0,  '403', 'HONGOS', ".$id_act_produccion.")";
				echoif("act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_detalle HONGOS failed: ");
					$Errores['act_recoleccion_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_recoleccion_detalle = $Tconn->insert_id;
					echoif(" id act_recoleccion_detalle HONGOS:".$id_act_recoleccion_detalle."\n");
				}
				/* act_recoleccion_act_recoleccion_detalle */
				$ins_act_recoleccion_act_recoleccion_detalle = " insert into act_recoleccion_act_recoleccion_detalle (act_recoleccion_detalles_id, act_recoleccion_detalle_id) ";
				$ins_act_recoleccion_act_recoleccion_detalle .= " values (".$id_actividad_principal.", ".$id_act_recoleccion_detalle." )";
				echoif("act_recoleccion_act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_act_recoleccion_detalle HONGOS failed: ");
					$Errores['act_recoleccion_act_recoleccion_detalle']++;
					echoif("\n\n");
				}
			}
			/* MIEL */
			if (! empty($row_tit['PRodRecoleccionMielVolumen']) && 
				  empty($row_tit['PRodRecoleccionMielUnidad']) &&
				  empty($row_tit['PRodRecoleccionMielAutoConsumo']) ) {
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PRodRecoleccionMielCanal'])."', ".$row_tit['PRodRecoleccionMielAutoConsumo'].", ".$row_tit['PRodRecoleccionMielMercado'].", ".$row_tit['PRodRecoleccionMielIntercambio'] . ", " . $row_tit['PRodRecoleccionMielPrecio'] . ", ".$row_tit['PRodRecoleccionMielVolumen'] . ", ".$row_tit['PRodRecoleccionMielUnidad'].", '". $row_tit['PRodRecoleccionMielExplotacion']."' )";
				echoif("act_produccion MIEL\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion MIEL failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_recoleccion_detalle */ 
				$ins_act_recoleccion_detalle = " insert into act_recoleccion_detalle (version, codigo, descripcion, produccion_id) ";
				$ins_act_recoleccion_detalle .= " values (0,  '404', 'MIEL', ".$id_act_produccion.")";
				echoif("act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_detalle MIEL failed: ");
					$Errores['act_recoleccion_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_recoleccion_detalle = $Tconn->insert_id;
					echoif(" id act_recoleccion_detalle MIEL:".$id_act_recoleccion_detalle."\n");
				}
				/* act_recoleccion_act_recoleccion_detalle */
				$ins_act_recoleccion_act_recoleccion_detalle = " insert into act_recoleccion_act_recoleccion_detalle (act_recoleccion_detalles_id, act_recoleccion_detalle_id) ";
				$ins_act_recoleccion_act_recoleccion_detalle .= " values (".$id_actividad_principal.", ".$id_act_recoleccion_detalle." )";
				echoif("act_recoleccion_act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_act_recoleccion_detalle MIEL failed: ");
					$Errores['act_recoleccion_act_recoleccion_detalle']++;
					echoif("\n\n");
				}
			}
			/* OTRO */
			if (! empty($row_tit['PRodRecoleccionOtroVolumen']) && 
				  empty($row_tit['PRodRecoleccionOtroUnidad']) &&
				  empty($row_tit['PRodRecoleccionOtroAutoConsumo']) ) {
			
				/* act_produccion */
				$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
				$ins_act_produccion .= " values (0, '".trim($row_tit['PRodRecoleccionOtroCanal'])."', ".$row_tit['PRodRecoleccionOtroAutoConsumo'].", ".$row_tit['PRodRecoleccionOtroMercado'].", ".$row_tit['PRodRecoleccionOtroIntercambio'] . ", " . $row_tit['PRodRecoleccionOtroPrecio'] . ", ".$row_tit['PRodRecoleccionOtroVolumen'] . ", ".$row_tit['PRodRecoleccionOtroUnidad'].", '". $row_tit['PRodRecoleccionOtroExplotacion']."' )";
				echoif("act_produccion OTRO\n");
				echoif($ins_act_produccion."\n");
				if (!$Tconn->query($ins_act_produccion)) {
					pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion OTRO failed: ");$Errores['act_produccion']++;echoif("\n\n");
				} else {    
					$id_act_produccion = $Tconn->insert_id;
					echoif(" id act_produccion :".$id_act_produccion."\n");
				}
				/* act_recoleccion_detalle */ 
				$ins_act_recoleccion_detalle = " insert into act_recoleccion_detalle (version, codigo, descripcion, produccion_id) ";
				$ins_act_recoleccion_detalle .= " values (0,  '405', 'OTRO', ".$id_act_produccion.")";
				echoif("act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_detalle OTRO failed: ");
					$Errores['act_recoleccion_detalle']++;
					echoif("\n\n");
				} else {    
					$id_act_recoleccion_detalle = $Tconn->insert_id;
					echoif(" id act_recoleccion_detalle OTRO:".$id_act_recoleccion_detalle."\n");
				}
				/* act_recoleccion_act_recoleccion_detalle */
				$ins_act_recoleccion_act_recoleccion_detalle = " insert into act_recoleccion_act_recoleccion_detalle (act_recoleccion_detalles_id, act_recoleccion_detalle_id) ";
				$ins_act_recoleccion_act_recoleccion_detalle .= " values (".$id_actividad_principal.", ".$id_act_recoleccion_detalle." )";
				echoif("act_recoleccion_act_recoleccion_detalle\n");
				echoif($ins_act_recoleccion_act_recoleccion_detalle."\n");
				if (!$Tconn->query($ins_act_recoleccion_act_recoleccion_detalle)) {
					pdberror($Tconn, "INSERT act_recoleccion_act_recoleccion_detalle OTRO failed: ");
					$Errores['act_recoleccion_act_recoleccion_detalle']++;
					echoif("\n\n");
				}
			}				
		}

		/*********************************************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_TURISMO_RURAL = 'actPrincipalTurismoRural', 'renaf.ActTurismoRural' */
		/*********************************************************************************************************/
		if ($row_tit['ProdTirismoRural']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalTurismoRural', 'renaf.ActTurismoRural', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalTurismoRural\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalTurismoRural failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalTurismoRural)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalTurismoRural failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_turismo_rural_detalle   rel   act_turismo_rural_act_turismo_rural_detalle */
			/* DENTRO DEL PREDIO */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
			$ins_act_produccion .= " values (0, '', 0, 0, 0, 0, 0, 0, '' )";
			echoif("act_produccion DENTRO DEL PREDIO\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion DENTRO DEL PREDIO failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_turismo_rural_detalle */ 
			$ins_act_turismo_rural_detalle = " insert into act_turismo_rural_detalle (version, codigo, descripcion, ingreso_bruto_anual, realiza, tipo_explotacion) ";
			$ins_act_turismo_rural_detalle .= " values (0,  '501', 'DENTRO DEL PREDIO', ".$row_tit['PTRenPredioIngresos'].", ".$row_tit['PTRenPredio'].", '".$row_tit['PTRenPredioExplotacion']."' )";
			echoif("act_turismo_rural_detalle\n");
			echoif($ins_act_turismo_rural_detalle."\n");
			if (!$Tconn->query($ins_act_turismo_rural_detalle)) {
				pdberror($Tconn, "INSERT act_turismo_rural_detalle DENTRO DEL PREDIO failed: ");
				$Errores['act_turismo_rural_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_turismo_rural_detalle = $Tconn->insert_id;
				echoif(" id act_turismo_rural_detalle DENTRO DEL PREDIO:".$id_act_turismo_rural_detalle."\n");
			}
			/* act_turismo_rural_act_turismo_rural_detalle */
			$ins_act_turismo_rural_act_turismo_rural_detalle = " insert into act_turismo_rural_act_turismo_rural_detalle (act_turismo_rural_detalles_id, act_turismo_rural_detalle_id) ";
			$ins_act_turismo_rural_act_turismo_rural_detalle .= " values (".$id_actividad_principal.", ".$id_act_turismo_rural_detalle." )";
			echoif("act_turismo_rural_act_turismo_rural_detalle\n");
			echoif($ins_act_turismo_rural_act_turismo_rural_detalle."\n");
			if (!$Tconn->query($ins_act_turismo_rural_act_turismo_rural_detalle)) {
				pdberror($Tconn, "INSERT act_turismo_rural_act_turismo_rural_detalle DENTRO DEL PREDIO failed: ");
				$Errores['act_turismo_rural_act_turismo_rural_detalle']++;
				echoif("\n\n");
			}
			/* FUERA DEL PREDIO */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
			$ins_act_produccion .= " values (0, '', 0, 0, 0, 0, 0, 0, '' )";
			echoif("act_produccion FUERA DEL PREDIO\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion FUERA DEL PREDIO failed: ");$Errores['act_produccion']++;echoif("\n\n");
			} else {    
				$id_act_produccion = $Tconn->insert_id;
				echoif(" id act_produccion :".$id_act_produccion."\n");
			}
			/* act_turismo_rural_detalle */ 
			$ins_act_turismo_rural_detalle = " insert into act_turismo_rural_detalle (version, codigo, descripcion, ingreso_bruto_anual, realiza, tipo_explotacion) ";
			$ins_act_turismo_rural_detalle .= " values (0,  '502', 'FUERA DEL PREDIO', ".$row_tit['PTRfueraPredioIngresos'].", ".$row_tit['PTRfueraPredio'].", '".$row_tit['PTRFueraPredioExplotacion']."' )";
			echoif("act_turismo_rural_detalle\n");
			echoif($ins_act_turismo_rural_detalle."\n");
			if (!$Tconn->query($ins_act_turismo_rural_detalle)) {
				pdberror($Tconn, "INSERT act_turismo_rural_detalle FUERA DEL PREDIO failed: ");
				$Errores['act_turismo_rural_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_turismo_rural_detalle = $Tconn->insert_id;
				echoif(" id act_turismo_rural_detalle FUERA DEL PREDIO:".$id_act_turismo_rural_detalle."\n");
			}
			/* act_turismo_rural_act_turismo_rural_detalle */
			$ins_act_turismo_rural_act_turismo_rural_detalle = " insert into act_turismo_rural_act_turismo_rural_detalle (act_turismo_rural_detalles_id, act_turismo_rural_detalle_id) ";
			$ins_act_turismo_rural_act_turismo_rural_detalle .= " values (".$id_actividad_principal.", ".$id_act_turismo_rural_detalle." )";
			echoif("act_turismo_rural_act_turismo_rural_detalle\n");
			echoif($ins_act_turismo_rural_act_turismo_rural_detalle."\n");
			if (!$Tconn->query($ins_act_turismo_rural_act_turismo_rural_detalle)) {
				pdberror($Tconn, "INSERT act_turismo_rural_act_turismo_rural_detalle FUERA DEL PREDIO failed: ");
				$Errores['act_turismo_rural_act_turismo_rural_detalle']++;
				echoif("\n\n");
			}
		}

		/********************************************************************************************/
		/* actividad_principal ACT_PRINCIPAL_PASTOREO = 'actPrincipalPastoreo', 'renaf.ActPastoreo' */
		/********************************************************************************************/
		if ($row_tit['ProdAnimal']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalPastoreo', 'renaf.ActPastoreo', null, null, ".$row_tit['ProdAnimalOrganicaCertificada'].", ".$row_tit['ProdAnimalOrganica'].")";
			echoif("actividad_principal actPrincipalPastoreo\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_principal."\n"."INSERT actividad_principal actPrincipalPastoreo failed: ");
				$Errores['actividad_principal']++;
				echoif("\n\n");
			} else {    
				$id_actividad_principal = $Tconn->insert_id;
				echoif(" id actividad_principal :".$id_actividad_principal."\n");
			}
			/* actividad_completa_actividad_principal */
			$ins_actividad_completa_actividad_principal  = " insert into actividad_completa_actividad_principal (actividad_completa_principales_id, actividad_principal_id) ";
			$ins_actividad_completa_actividad_principal .= " values (".$id_actividad_completa.", ".$id_actividad_principal.")";
			echoif("actividad_completa_actividad_principal (actPrincipalPastoreo)\n");
			echoif($ins_actividad_completa_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_principal)) {
				pdberror($Tconn, $ins_actividad_completa_actividad_principal."\n"."INSERT actividad_completa_actividad_principal actPrincipalPastoreo failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_pastoreo_detalle   rel   act_pastoreo_act_pastoreo_detalle */
			if ($nRows_pa > 0) {
				$pa=0;
				while ($row_pa = mysqli_fetch_array($res_pa)) {
					$pa++;
					
					/* act_produccion */
					$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )";
					$ins_act_produccion .= " values (0, '".trim($row_pa['PACanal'])."', ".$row_pa['PAAutoConsumo'].", ".$row_pa['PAMercado'].", ".$row_pa['PAIntercambio'].", ".$row_pa['PAPrecioUnitario'].", ".$row_pa['PAVolumen'].", ".$row_pa['PAUnidad'].", '".$row_pa['PAExplotacion']."' )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, $ins_act_produccion."\n"."INSERT act_produccion failed: ");$Errores['act_produccion']++;echoif("\n\n");
					} else {    
						$id_act_produccion = $Tconn->insert_id;
						echoif(" id act_produccion :".$id_act_produccion."\n");
					}
					/* act_pastoreo_detalle */
					$ins_act_pastoreo_detalle = " insert into act_pastoreo_detalle (version, cantidad_de_animales, cantidad_de_vientres, codigo, descripcion, produccion_id) ";
					$ins_act_pastoreo_detalle .= " values (0, ".$row_pa['PACabezas'].", ".$row_pa['PAVientres'].", '".$row_pa['act_cod']."', '".$row_pa['act_desc']."', ".$id_act_produccion." )";
					echoif("act_pastoreo_detalle\n");
					echoif($ins_act_pastoreo_detalle."\n");
					if (!$Tconn->query($ins_act_pastoreo_detalle)) {
						pdberror($Tconn, $ins_act_pastoreo_detalle."\n"."INSERT act_pastoreo_detalle failed: ");
						$Errores['act_pastoreo_detalle']++;
						echoif("\n\n");
					} else {    
						$id_act_pastoreo_detalle = $Tconn->insert_id;
						echoif(" id act_pastoreo_detalle :".$id_act_pastoreo_detalle."\n");
					}
					/* act_pastoreo_act_pastoreo_detalle */
					$ins_act_pastoreo_act_pastoreo_detalle = " insert into act_pastoreo_act_pastoreo_detalle (act_pastoreo_detalles_id, act_pastoreo_detalle_id) ";
					$ins_act_pastoreo_act_pastoreo_detalle .= " values (".$id_actividad_principal.", ".$id_act_pastoreo_detalle." )";
					echoif("act_pastoreo_act_pastoreo_detalle\n");
					echoif($ins_act_pastoreo_act_pastoreo_detalle."\n");
					if (!$Tconn->query($ins_act_pastoreo_act_pastoreo_detalle)) {
						pdberror($Tconn, $ins_act_pastoreo_act_pastoreo_detalle."\n"."INSERT act_pastoreo_act_pastoreo_detalle failed: ");
						$Errores['act_pastoreo_act_pastoreo_detalle']++;
						echoif("\n\n");
					}
					/* sub_producto_animal   rel   act_pastoreo_detalle_sub_producto_animal */
					/* sub_producto_animal */
					if (!empty($row_pa['PASubCodigo'])) {
						$ins_sub_producto_animal = " insert into sub_producto_animal (version, produccion_id, tipo_subproducto_id) ";
						$ins_sub_producto_animal .= " values (0, ".$id_act_produccion.", ".$row_pa['PASubCodigo']." )";
						echoif("sub_producto_animal subcodigo[".$row_pa['PASubCodigo']."]\n");
						echoif($ins_sub_producto_animal."\n");
						if (!$Tconn->query($ins_sub_producto_animal)) {
							pdberror($Tconn, $ins_sub_producto_animal."\n"."INSERT sub_producto_animal failed: ");
							$Errores['sub_producto_animal']++;
							echoif("\n\n");
						} else {    
							$id_sub_producto_animal = $Tconn->insert_id;
							echoif(" id sub_producto_animal :".$id_sub_producto_animal."\n");
						}
						/* act_pastoreo_detalle_sub_producto_animal */
						$ins_act_pastoreo_detalle_sub_producto_animal = " insert into act_pastoreo_detalle_sub_producto_animal ( act_pastoreo_detalle_subproductos_id, sub_producto_animal_id ) ";
						$ins_act_pastoreo_detalle_sub_producto_animal .= " values (".$id_act_pastoreo_detalle.", ".$id_sub_producto_animal." )";
						echoif("act_pastoreo_detalle_sub_producto_animal\n");
						echoif($ins_act_pastoreo_detalle_sub_producto_animal."\n");
						if (!$Tconn->query($ins_act_pastoreo_detalle_sub_producto_animal)) {
							pdberror($Tconn, $ins_act_pastoreo_detalle_sub_producto_animal."\n"."INSERT act_pastoreo_detalle_sub_producto_animal failed: ");
							$Errores['act_pastoreo_detalle_sub_producto_animal']++;
							echoif("\n\n");
						}
					} else {
						echoif("sin sub_producto_animal\n");
					}
				}
			} else {
				echoif("sin pastoreo\n");
			}
		}	
		
		  /**********/		 
		 /* limite */
		/**********/
		$ins_limite = " insert into limite (version, tiene_limites) ";
		$ins_limite .= " values (0, ".$row_tierra['LimitesDefinidos']." )";
		echoif("limite \n");
		echoif($ins_limite."\n");
		if (!$Tconn->query($ins_limite)) {
			pdberror($Tconn, $ins_limite."\n"."INSERT limite failed: ");
			$Errores['limite']++;
			echoif("\n\n");
		} else {    
			$id_limite = $Tconn->insert_id;
			echoif(" id limite :".$id_limite."\n");
		}
		if ($row_tierra['LimitesDefinidos']) {
			/* PROPIETARIO(0, "Propietario", "opPropietario"), */ 
			if ($row_tierra['hsPropietario'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsPropietario'], $row_tierra['hsPropietarioUn'], "Propietario");
			}
			/* ARRENDATARIO(1, "Arrendatario", "opArrandario"),  */
			if ($row_tierra['hsArrendatario'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsArrendatario'], $row_tierra['hsArrendatarioUn'], "Arrendatario");
			}
			/* EN_MEDIERIA(2, "En mediería", "opMedireria"), */ 
			if ($row_tierra['hsMedieria'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsMedieria'], $row_tierra['hsMedieriaUn'], "En mediería");
			}
			/* EN_APARCERIA(3, "En aparcería", "opAparceria"), */ 
			if ($row_tierra['hsAparceria'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsAparceria'], $row_tierra['hsAparceriaUn'], "En aparcería");
			}
			/* CODOMINIOS(4, "￼Condominios hereditarios indivisos (en sucesión)", "opCodominios"), */ 
			if ($row_tierra['hsCondominio'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsCondominio'], $row_tierra['hsCondominioUn'], "￼Condominios hereditarios indivisos (en sucesión)");
			}
			/* CONTRATO_ACCIDENTAL(5, "Contrato accidental", "opContratoAccidental"), */ 
			if ($row_tierra['hsContrato'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsContrato'], $row_tierra['hsContratoUn'], "Contrato accidental");
			}
			/* TIERRAS_PRIVADAS(6, "Posesión en tierras privadas", "opTierrasPrivadas"), */ 
			if ($row_tierra['hsPPrivadas'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsPPrivadas'], $row_tierra['hsPPrivadasUn'], "Posesión en tierras privadas");
			}
			/* TIERRAS_FISCALES(7, "Tenencia en tierras fiscales", "opTierrasFiscales"), */ 
			if ($row_tierra['hsTFiscales'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsTFiscales'], $row_tierra['hsTFiscalesUn'], "Tenencia en tierras fiscales");
			}
			/* hsTPrivadas	hsTPrivadasUn FALTA! en la tabla tierra origen */
			/* INTEGRANTES(8, "Integrantes", "opIntegrantes"), */ 
			if ($row_tierra['hsIntegrante'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsIntegrante'], $row_tierra['hsIntegranteUn'], "Integrantes");
			}
			/* POSESION(9, "Posesión comunitaria indígena", "opPosesion"), */ 
			if ($row_tierra['hsPosesionComunitariaIndigena'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsPosesionComunitariaIndigena'], $row_tierra['hsPosesionComunitariaIndigenaUn'], "Posesión comunitaria indígena");
			}
			/* PROPIEDAD(10, "Propiedad comunitaria indígena", "opPropiedad"), */ 
			if ($row_tierra['hsPropiedadComunitariaIndigena'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsPropiedadComunitariaIndigena'], $row_tierra['hsPropiedadComunitariaIndigenaUn'], "Propiedad comunitaria indígena");
			}
			/* OTRA(11, "OTRA", "opOtra"), */ 
			if ($row_tierra['hsOtro'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsOtro'], $row_tierra['hsOtroUn'], "OTRA");
			}
			/* SUPERFICIE_TOTAL(12, "SUPERFICIE TOTAL", "opSuperficieTotal"), */
			if ($row_tierra['hsTotal'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['hsTotal'], $row_tierra['hsTotalUn'], "SUPERFICIE TOTAL");
			}
			/* SUPERFICIE_REAL(13, "Superficie real trabajada por el NAFa", */ 
			if ($row_tierra['supRealTrabajada'] > 0) {
				ins_explotacion_con_limite($Tconn, $id_limite, $row_tierra['supRealTrabajada'], $row_tierra['supRealTrabajadaUn'], "Superficie real trabajada por el NAF");
			}

		} else {
			/* POSESION(0, "Posesion", "opPosesionS"), */
			ins_explotacion_sin_limite($Tconn, $id_limite, "Posesion", $row_tierra['ESDPosesionComunero'], $row_tierra['ESDPosesionParque'], $row_tierra['ESDPosesionTierraFiscal'], $row_tierra['ESDPosesionOtros']);
			/* TENENCIA(1, "Tenencia", "opPropietarioS"), */ 
			ins_explotacion_sin_limite($Tconn, $id_limite, "Tenencia", $row_tierra['ESDTenenciaComunero'], $row_tierra['ESDTenenciaParque'], $row_tierra['ESDTenenciaTierraFiscal'], $row_tierra['ESDTenenciaOtros']);
			/* ARRENDATARIO(2, "Arrendatario", "opArrandarioS"), */ 
			ins_explotacion_sin_limite($Tconn, $id_limite, "Arrendatario", $row_tierra['ESDArrendamientoComunero'], $row_tierra['ESDArrendamientoParque'], $row_tierra['ESDArrendamientoTierraFiscal'], $row_tierra['ESDArrendamientoOtros']);
			/* EN_APARCERIA(3, "En aparcería", "opAparceriaS"),  */
			ins_explotacion_sin_limite($Tconn, $id_limite, "En aparcería", $row_tierra['ESDAparceriaComunero'], $row_tierra['ESDAparceriaParque'], $row_tierra['ESDAparceriaTierraFiscal'], $row_tierra['ESDAparceriaOtros']);
			/* DERECHOSO(4, "Derechoso", "opDerechosoS"),  */
			ins_explotacion_sin_limite($Tconn, $id_limite, "Derechoso", $row_tierra['ESDDerechosoComunero'], $row_tierra['ESDDerechosoParque'], $row_tierra['ESDDerechosoTierraFiscal'], $row_tierra['ESDDerechosoOtros']);
			/* INTEGRANTE(5, "Integrante", "opIntegranteS"),  */
			ins_explotacion_sin_limite($Tconn, $id_limite, "Integrante", $row_tierra['ESDIntegranteComunero'], $row_tierra['ESDIntegranteParque'], $row_tierra['ESDIntegranteTierraFiscal'], $row_tierra['ESDIntegranteOtros']);
			/* POSESION_INDIGENA(6, "Posesión comunitaria indígena", "opPosesionIndigenaS"), */
			ins_explotacion_sin_limite($Tconn, $id_limite, "Posesión comunitaria indígena", $row_tierra['ESDPosesionComunitariaIndigenaComunero'], $row_tierra['ESDPosesionComunitariaIndigenaParque'], $row_tierra['ESDPosesionComunitariaIndigenaTierraFiscal'], $row_tierra['ESDPosesionComunitariaIndigenaOtros']); 			
						
		} 

		  /**********/
		 /* tierra */
		/**********/
		$ins_tierra = " insert into tierra (version, asociativa, compartido, compartido_con_cuantos, limite_id) ";
		$ins_tierra .= " values (0, ".$row_tierra['ExplotacionAsociativa'].", ".$row_tierra['Compartida'].", ".$row_tierra['ComparteCon'].", ".$id_limite." )";
		echoif("tierra \n");
		echoif($ins_tierra."\n");
		if (!$Tconn->query($ins_tierra)) {
			pdberror($Tconn, $ins_tierra."\n"."INSERT tierra failed: ");
			$Errores['tierra']++;
			echoif("\n\n");
		} else {    
			$id_tierra = $Tconn->insert_id;
			echoif(" id tierra :".$id_tierra."\n");
		}
		
		  /**************************************/
		 /* tipo_juridico tierra_tipo_juridico */
		/**************************************/
		/* ASOCIACION_CIVIL(0, "Asociación Civil"), */  
		if ($row_tierra['AsocAsociacionCivil']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['AsocCivilNombre'], "0");
	 	}
	 	/* COOPERATIVA(1, "Cooperativa"), */
		if ($row_tierra['AsocCooperativa']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['CooperativaNombre'], "1");
		}
	 	/* COOPERADORA(2, "Cooperadora"), */ 
		if ($row_tierra['AsocCooperadora']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['CooperadoraNombre'], "2");
		}
	 	/* CONSORCIO(3, "Consorcio"), */ 
		if ($row_tierra['AsocConsorcio']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['ConsorcioNombre'], "3");
		}
		/* COMUNIDAD(4, "Comunidad de pueblo originario Civil"), */
		if ($row_tierra['AsocComunidad']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['ComunidadNombre'], "4");
		}
		/* HUERTA_COMUNITARIA(5, "Huerta comunitaria"), */ 
		if ($row_tierra['AsocHuerta']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['HuertaNombre'], "5");
		}
		/* GRUPO(6, "Grupo"), */ 
		if ($row_tierra['AsocGrupo']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['GrupoNombre'], "6");
		}
		/* SOCIEDAD_DE_HECHO(7, "Sociedad de hecho") */
		if ($row_tierra['AsocSociedad']) {
			ins_tipo_juridico($Tconn, $id_tierra, $row_tierra['SociedadNombre'], "7");
		}												

		  /************************/
		 /* riesgo tierra_riesgo */
		/************************/
		if (!$row_tierra['RiesgosGranizo']) {
			ins_riesgo($Tconn, $id_tierra, 'granizo', 'Granizo', $row_tierra['AsocSociedad']);
		}
		if (!$row_tierra['RiesgosHelada']) {
			ins_riesgo($Tconn, $id_tierra, 'helada', 'Helada', $row_tierra['RiesgosHelada']);
		}
		if (!$row_tierra['RiesgosInundacion']) {
			ins_riesgo($Tconn, $id_tierra, 'inundacion', 'Inundación', $row_tierra['RiesgosInundacion']);
		}
		if (!$row_tierra['RiesgosVientos']) {
			ins_riesgo($Tconn, $id_tierra, 'vientosFuertes', 'Vientos fuertes', $row_tierra['RiesgosVientos']);
		}
		if (!$row_tierra['RiesgosExceso']) {
			ins_riesgo($Tconn, $id_tierra, 'exesoHidrico', 'Exeso hídrico', $row_tierra['RiesgosExceso']);
		}
		if (!$row_tierra['RiesgosDeficit']) {
			ins_riesgo($Tconn, $id_tierra, 'deficitHidrico', 'Déficit hídrico', $row_tierra['RiesgosDeficit']);
		}
		if (!$row_tierra['RiesgosIncendio']) {
			ins_riesgo($Tconn, $id_tierra, 'incendio', 'Incendio', $row_tierra['RiesgosIncendio']);
		}
		if (!$row_tierra['RiesgosBiologicos']) {
			ins_riesgo($Tconn, $id_tierra, 'biologicos', 'Biológicos / Sanitarios', $row_tierra['RiesgosBiologicos']);
		}
		if (!$row_tierra['RiesgosComerciales']) {
			ins_riesgo($Tconn, $id_tierra, 'comerciales', 'Comerciales', $row_tierra['RiesgosComerciales']);
		}
		if (!$row_tierra['RiesgosFinancieros']) {
			ins_riesgo($Tconn, $id_tierra, 'financieros', 'Financieros', $row_tierra['RiesgosFinancieros']);
		}
 
		  /********************************/
		 /* prevencion tierra_prevencion */
		/********************************/
		if (!$row_tierra['PrevencionMalla']) {
			ins_prevencion ($Tconn, $id_tierra, $row_tierra['PrevencionMalla'], 'mallaAntigranizo', '¿Utilizan malla antigranizo?' );
		} 
		if (!$row_tierra['PrevencionRiego'] || !$row_tierra['PrevencionQuemadores']) {
			ins_prevencion ($Tconn, $id_tierra, $row_tierra['PrevencionRiego'], 'aspersionEnHeladas', '¿Utilizan quemadores / riego por aspersión en heladas?' );
		}
		if (!$row_tierra['PrevencionObras']) {
			ins_prevencion ($Tconn, $id_tierra, $row_tierra['PrevencionObras'], 'canales', '¿Utilizan canales, terraplenes, defensas, represas, etc.?' );
		}
		if (!$row_tierra['PrevencionCobertura']) {
			ins_prevencion ($Tconn, $id_tierra, $row_tierra['PrevencionCobertura'], 'coberturaRiesgos', '¿Contrata cobertura de riesgos (seguro / fondo para contingencias/etc.)?' );
		} 
		if (!$row_tierra['PrevencionOtra']) {
			ins_prevencion ($Tconn, $id_tierra, $row_tierra['PrevencionOtra'], 'otra', $row_tierra['PrevencionOtraTexto'] );
		}

		  /* * * * * * * * * * */
		 /*  R E C U R S O S  */
		/* * * * * * * * * * */

		/* rec_riego */
		$id_superficie = ins_superficie($Tconn, $row_tit['RiegoSuperficie'], $row_tit['RiegoUnidades']); 
		switch ($row_tit['RiegoTipo']) {
			case '1':
				$tipoRiego = "Superficial";
        		break;
		    case '2':
		        $tipoRiego = "Goteo";
		        break;
		    case '3':
		        $tipoRiego = "Aspersión";
		        break;
		    case '4':
		        $tipoRiego = "Manual";
		        break;
		    case 'x':
		        $tipoRiego = "N/A";
		        break;
		}
		$ins_riego = " insert into rec_riego (version, superficie_id, tipo_riego, utiliza) ";
		$ins_riego .= " values (0, ".$id_superficie.", '".$tipoRiego."', ".$row_tit['Riego']." )";
		echoif("rec_riego\n");
		echoif($ins_riego."\n");
		if (!$Tconn->query($ins_riego)) {
			pdberror($Tconn, $ins_riego."\n"."INSERT rec_riego failed: ");
			$Errores['rec_riego']++;
			echoif("\n\n");
		} else {    
			$id_riego = $Tconn->insert_id;
			echoif(" id riego :".$id_riego."\n");
		}
		/* recursos */
		$ins_recursos = " insert into recursos (version, riego_id, utiliza_agua_para_consumo_animal, utiliza_traccion_animal_para_producir) ";
		$ins_recursos .= " values (0, ".$id_riego.", ".$row_tit['AguaConsumoAnimal'].", ".$row_tit['TraccionAnimal']." )";
		echoif("recursos\n");
		echoif($ins_recursos."\n");
		if (!$Tconn->query($ins_recursos)) {
			pdberror($Tconn, $ins_recursos."\n"."INSERT recursos failed: ");
			$Errores['recursos']++;
			echoif("\n\n");
		} else {    
			$id_recursos = $Tconn->insert_id;
			echoif(" id recursos :".$id_recursos."\n");
		}
		/* rec_tractores */
		/* 1 */
		switch ($row_tit['TR1Adquisicion']) {
			case '1':
				$adquisicion_particular = 1;$adquisicion_subsidiado = 0;
        		break;
			case '2':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 1;
        		break;
			case 'x':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 0;
        		break;
   		}
		$modelo = mysqli_real_escape_string($Tconn, $row_tit['TR1Modelo']);
 		$ins_rec_tractores = " insert into rec_tractores (version, adquisicion_particular, adquisicion_subsidiado, cantidad, modelo, potencia_enhp, recursos_id, uso) ";
		$ins_rec_tractores .= " values (0, ".$adquisicion_particular.", ".$adquisicion_subsidiado.", ".$row_tit['TR1Cantidad'].", '".$modelo."', ".$row_tit['TR1Potencia'].", ".$id_recursos.", '".$row_tit['TR1Propiedad']."' )";
		echoif("rec_tractores 1\n");
		echoif($ins_rec_tractores."\n");
		if (!$Tconn->query($ins_rec_tractores)) {
			pdberror($Tconn, $ins_rec_tractores."\n"."INSERT rec_tractores failed: ");
			$Errores['rec_tractores']++;
			echoif("\n\n");
		}
		/* 2 */
		switch ($row_tit['TR2Adquisicion']) {
			case '1':
				$adquisicion_particular = 1;$adquisicion_subsidiado = 0;
        		break;
			case '2':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 1;
        		break;
			case 'x':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 0;
        		break;
   		}
		$modelo = mysqli_real_escape_string($Tconn, $row_tit['TR2Modelo']);
 		$ins_rec_tractores = " insert into rec_tractores (version, adquisicion_particular, adquisicion_subsidiado, cantidad, modelo, potencia_enhp, recursos_id, uso) ";
		$ins_rec_tractores .= " values (0, ".$adquisicion_particular.", ".$adquisicion_subsidiado.", ".$row_tit['TR2Cantidad'].", '".$modelo."', ".$row_tit['TR2Potencia'].", ".$id_recursos.", '".$row_tit['TR2Propiedad']."' )";
		echoif("rec_tractores 2\n");
		echoif($ins_rec_tractores."\n");
		if (!$Tconn->query($ins_rec_tractores)) {
			pdberror($Tconn, $ins_rec_tractores."\n"."INSERT rec_tractores failed: ");
			$Errores['rec_tractores']++;
			echoif("\n\n");
		}
		/* rec_vehiculo */
		/* 1 */
		switch ($row_tit['VH1Adquisicion']) {
			case '1':
				$adquisicion_particular = 1;$adquisicion_subsidiado = 0;
        		break;
			case '2':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 1;
        		break;
			case 'x':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 0;
        		break;
   		}
		$modelo = mysqli_real_escape_string($Tconn, $row_tit['VH1Modelo']);
 		$ins_rec_vehiculo = " insert into rec_vehiculo (version, adquisicion_particular, adquisicion_subsidiado, cantidad, modelo, recursos_id, tipo, uso) ";
		$ins_rec_vehiculo .= " values (0, ".$adquisicion_particular.", ".$adquisicion_subsidiado.", ".$row_tit['VH1Cantidad'].", '".$modelo."', ".$id_recursos.", '".$row_tit['VH1Tipo']."', '".$row_tit['VH1Propiedad']."' )";
		echoif("rec_vehiculo 1\n");
		echoif($ins_rec_vehiculo."\n");
		if (!$Tconn->query($ins_rec_vehiculo)) {
			pdberror($Tconn, $ins_rec_vehiculo."\n"."INSERT rec_vehiculo failed: ");
			$Errores['rec_vehiculo']++;
			echoif("\n\n");
		}
		/* 2 */
		switch ($row_tit['VH2Adquisicion']) {
			case '1':
				$adquisicion_particular = 1;$adquisicion_subsidiado = 0;
        		break;
			case '2':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 1;
        		break;
			case 'x':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 0;
        		break;
   		}
		$modelo = mysqli_real_escape_string($Tconn, $row_tit['VH2Modelo']);
 		$ins_rec_vehiculo = " insert into rec_vehiculo (version, adquisicion_particular, adquisicion_subsidiado, cantidad, modelo, recursos_id, tipo, uso) ";
		$ins_rec_vehiculo .= " values (0, ".$adquisicion_particular.", ".$adquisicion_subsidiado.", ".$row_tit['VH2Cantidad'].", '".$modelo."', ".$id_recursos.", '".$row_tit['VH2Tipo']."', '".$row_tit['VH2Propiedad']."' )";
		echoif("rec_vehiculo 2\n");
		echoif($ins_rec_vehiculo."\n");
		if (!$Tconn->query($ins_rec_vehiculo)) {
			pdberror($Tconn, $ins_rec_vehiculo."\n"."INSERT rec_vehiculo failed: ");
			$Errores['rec_vehiculo']++;
			echoif("\n\n");
		}
		
		/* rec_embarcaciones */
		/* 1 */
		switch ($row_tierra['EMB1Adquisicion']) {
			case '1':
				$adquisicion_particular = 1;$adquisicion_subsidiado = 0;
        		break;
			case '2':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 1;
        		break;
			case 'x':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 0;
        		break;
   		}
		switch ($row_tierra['EMB1Motor']) {
			case '1':
				$externo = 1;$interno = 0;
        		break;
			case '2':
				$externo = 0;$interno = 1;
        		break;
			case 'x':
				$externo = 0;$interno = 0;
        		break;
   		}
 		$ins_rec_embarcaciones = " insert into rec_embarcaciones (version, adquisicion_particular, adquisicion_subsidiado, cantidad, eslora, material, motor_externo, motor_interno, potencia, recursos_id, tipo, uso) ";
		$ins_rec_embarcaciones .= " values (0, ".$adquisicion_particular.", ".$adquisicion_subsidiado.", ".$row_tierra['EMB1Cantidad'].", ".$row_tierra['EMB1Eslora'].", '".$row_tierra['EMB1Material']."', ".$externo.", ".$interno.", ".$row_tierra['EMB1Potencia'].", ".$id_recursos.", '".$row_tierra['EMB1Tipo']."', '".$row_tierra['EMB1Uso']."' )";
		echoif("rec_embarcaciones 1\n");
		echoif($ins_rec_embarcaciones."\n");
		if (!$Tconn->query($ins_rec_embarcaciones)) {
			pdberror($Tconn, $ins_rec_embarcaciones."\n"."INSERT rec_embarcaciones failed: ");
			$Errores['rec_embarcaciones']++;
			echoif("\n\n");
		}
		/* 2 */
		switch ($row_tierra['EMB2Adquisicion']) {
			case '1':
				$adquisicion_particular = 1;$adquisicion_subsidiado = 0;
        		break;
			case '2':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 1;
        		break;
			case 'x':
				$adquisicion_particular = 0;$adquisicion_subsidiado = 0;
        		break;
   		}
		switch ($row_tierra['EMB2Motor']) {
			case '1':
				$externo = 1;$interno = 0;
        		break;
			case '2':
				$externo = 0;$interno = 1;
        		break;
			case 'x':
				$externo = 0;$interno = 0;
        		break;
   		}
 		$ins_rec_embarcaciones = " insert into rec_embarcaciones (version, adquisicion_particular, adquisicion_subsidiado, cantidad, eslora, material, motor_externo, motor_interno, potencia, recursos_id, tipo, uso) ";
		$ins_rec_embarcaciones .= " values (0, ".$adquisicion_particular.", ".$adquisicion_subsidiado.", ".$row_tierra['EMB2Cantidad'].", ".$row_tierra['EMB2Eslora'].", '".$row_tierra['EMB2Material']."', ".$externo.", ".$interno.", ".$row_tierra['EMB2Potencia'].", ".$id_recursos.", '".$row_tierra['EMB2Tipo']."', '".$row_tierra['EMB2Uso']."' )";
		echoif("rec_embarcaciones 2\n");
		echoif($ins_rec_embarcaciones."\n");
		if (!$Tconn->query($ins_rec_embarcaciones)) {
			pdberror($Tconn, $ins_rec_embarcaciones."\n"."INSERT rec_embarcaciones failed: ");
			$Errores['rec_embarcaciones']++;
			echoif("\n\n");
		}

		  /*********************/
		 /* mejora_produccion */
		/*********************/
		if ($nRows_in > 0) {
			$in=0;
			while ($row_in = mysqli_fetch_array($res_in)) {
				$in++;
				if( isset( $unidades_infraestructura[$row_in['InfraestructuraCodigo']] ) ) {
					/* mejora_produccion */
					switch ($row_in['adq_cod']) {
						case '1':
							$propia = 1;
							$subsidio = 0;
	        				break;
					    case '2':
							$propia = 0;
							$subsidio = 1;
					        break;
					    case '0':
							$propia = 0;
							$subsidio = 0;
			        		break;
					}
					$id_superficie = ins_superficie($Tconn, $row_in['InfraestructuraCantidad'], $unidades_infraestructura[$row_in['InfraestructuraCodigo']]);
					$ins_mejora_produccion = " insert into mejora_produccion (version, adquisicion_propia, adquisicion_subsidiado, cantidad_id, codigo, descripcion, recursos_id )"; 
					$ins_mejora_produccion .= " values (0, ".$propia.", ".$subsidio.", ".$id_superficie.", '".$row_in['act_cod']."', '".$row_in['act_desc']."', ".$id_recursos." )";
					echoif("mejora_produccion\n");
					echoif($ins_mejora_produccion."\n");
					if (!$Tconn->query($ins_mejora_produccion)) {
						pdberror($Tconn, $ins_mejora_produccion."\n"."INSERT mejora_produccion failed: ");$Errores['mejora_produccion']++;echoif("\n\n");
					} 
				}
			}
		}
		  /**************/
		 /* tecnologia */
		/**************/
		$ins_tecnologia = " insert into tecnologia (version, abonos_organicos_comprados, abonos_organicos_produccion_propia, 
abonos_organicos_subsidiados, abonos_quimicos_comprados, abonos_quimicos_subsidiados, control_plagas_no_quimicos, 
control_plagas_no_quimicos_biologicos, control_plagas_no_quimicos_otros, control_plagas_quimicos, 
mejoras_geneticas_cruzamiento, mejoras_geneticas_inseminacion_artificial, mejoras_geneticas_otras, 
mejoras_geneticas_seleccion, rotacion_cultivos, semillas_compradas, 
semillas_produccion_propia, semillas_subsidiadas) ";
		$ins_tecnologia .= " values (0, ".$row_tit['AbonosOrganicosCompra']." , ".
			$row_tit['AbonosOrganicosProduccion']." , ".
			$row_tit['AbonosOrganicosSubsidiado']." , ".
			$row_tit['AbonosQuimicosCompra']." , ".
			$row_tit['AbonosQuimicosSubsidiado']." , ".
			$row_tit['ControlPlagaNoQuimicos']." , ".
			$row_tit['ControlPlagaBiologicos']." , ".
			$row_tit['ControlPlagaOtrosMetodos']." , ".
			$row_tit['ControlPlagaQuimicos']." , ".
			$row_tit['AnimalesMejoraCruzamiento']." , ".
			$row_tit['AnimalesMejoraIA']." , '".
			$row_tit['AnimalesMejoraTexto']."' , ".
			$row_tit['AnimalesMejoraSeleccion']." , ".
			$row_tit['Rotacion']." , ".
			$row_tit['SemillasCompra']." , ".
			$row_tit['SemillasProduccioPropia']." , ".
			$row_tit['SemillasSubsudiado']." )".
		echoif("tecnologia\n");
		echoif($ins_tecnologia."\n");
		if (!$Tconn->query($ins_tecnologia)) {
			pdberror($Tconn, $ins_tecnologia."\n"."INSERT tecnologia failed: ");
			$Errores['tecnologia']++;
			echoif("\n\n");
		} else {    
			$id_tecnologia = $Tconn->insert_id;
			echoif(" id tecnologia :".$id_tecnologia."\n");
		}
		  /*************************/
		 /* mano_de_obra_completa */
		/*************************/

		/* cantidad_trabajadores_temporarios */
		/* Temporarios Completa */
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['TrabFamEventualCompleto1S']." ,".
			$row_tit['TrabFamEventualCompleto1Q']." ,".
			$row_tit['TrabFamEventualCompleto1M']." ,".
			$row_tit['TrabFamEventualCompleto2M']." ,".
			$row_tit['TrabFamEventualCompleto3M']." ,".
			$row_tit['TrabFamEventualCompleto4M']." ,".
			$row_tit['TrabFamEventualCompleto5M']." ,".
			$row_tit['TrabFamEventualCompleto6M']." )";
		echoif("cantidad_trabajadores_temporarios TrabFamEventualCompleto\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios TrabFamEventualCompleto failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_completa_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios TrabFamEventualCompleto:".$id_trabtemp_completa_id."\n");
		}
		/*Temporarios Parcial */
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['TrabFamEventualParcial1S']." ,".
			$row_tit['TrabFamEventualParcial1Q']." ,".
			$row_tit['TrabFamEventualParcial1M']." ,".
			$row_tit['TrabFamEventualParcial2M']." ,".
			$row_tit['TrabFamEventualParcial3M']." ,".
			$row_tit['TrabFamEventualParcial4M']." ,".
			$row_tit['TrabFamEventualParcial5M']." ,".
			$row_tit['TrabFamEventualParcial6M']." )";
		echoif("cantidad_trabajadores_temporarios TrabFamEventualParcial\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios TrabFamEventualParcial failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_parcial_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios TrabFamEventualParcial:".$id_trabtemp_parcial_id."\n");
		}
		/* Preparación del suelo */
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['Preparacion1S']." ,".
			$row_tit['Preparacion1Q']." ,".
			$row_tit['Preparacion1M']." ,".
			$row_tit['Preparacion2M']." ,".
			$row_tit['Preparacion3M']." ,".
			$row_tit['Preparacion4M']." ,".
			$row_tit['Preparacion5M']." ,".
			$row_tit['Preparacion6M']." )";
		echoif("cantidad_trabajadores_temporarios Preparacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios Preparacion failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_preparacion_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios Preparacion:".$id_trabtemp_preparacion_id."\n");
		}
		/* Siembra / plantación */
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['Siembra1S']." ,".
			$row_tit['Siembra1Q']." ,".
			$row_tit['Siembra1M']." ,".
			$row_tit['Siembra2M']." ,".
			$row_tit['Siembra3M']." ,".
			$row_tit['Siembra4M']." ,".
			$row_tit['Siembra5M']." ,".
			$row_tit['Siembra6M']." )";
		echoif("cantidad_trabajadores_temporarios Siembra\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios Siembra failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_siembra_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios Siembra:".$id_trabtemp_siembra_id."\n");
		}
		/* Labores culturales */
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['Labores1S']." ,".
			$row_tit['Labores1Q']." ,".
			$row_tit['Labores1M']." ,".
			$row_tit['Labores2M']." ,".
			$row_tit['Labores3M']." ,".
			$row_tit['Labores4M']." ,".
			$row_tit['Labores5M']." ,".
			$row_tit['Labores6M']." )";
		echoif("cantidad_trabajadores_temporarios Labores cult\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios Labores cult failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_labores_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios Labores cult:".$id_trabtemp_labores_id."\n");
		}
		/* Cosecha */
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['Cosecha1S']." ,".
			$row_tit['Cosecha1Q']." ,".
			$row_tit['Cosecha1M']." ,".
			$row_tit['Cosecha2M']." ,".
			$row_tit['Cosecha3M']." ,".
			$row_tit['Cosecha4M']." ,".
			$row_tit['Cosecha5M']." ,".
			$row_tit['Cosecha6M']." )";
		echoif("cantidad_trabajadores_temporarios Cosecha\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios Cosecha failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_cosecha_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios Cosecha:".$id_trabtemp_cosecha_id."\n");
		}
		/* Otras */ 				
		$ins_sql = " insert into cantidad_trabajadores_temporarios (version, hasta_una_semana, hasta_una_quincena, hasta_un_mes, hasta_dos_meses, hasta_tres_meses, hasta_cuatro_meses, hasta_cinco_meses, hasta_seis_meses) ";
		$ins_sql .= " values (0, ".
			$row_tit['Otras1S']." ,".
			$row_tit['Otras1Q']." ,".
			$row_tit['Otras1M']." ,".
			$row_tit['Otras2M']." ,".
			$row_tit['Otras3M']." ,".
			$row_tit['Otras4M']." ,".
			$row_tit['Otras5M']." ,".
			$row_tit['Otras6M']." )";
		echoif("cantidad_trabajadores_temporarios Otras\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT cantidad_trabajadores_temporarios Otras failed: ");
			$Errores['cantidad_trabajadores_temporarios']++;
			echoif("\n\n");
		} else {    
			$id_trabtemp_otras_id  = $Tconn->insert_id;
			echoif(" id cantidad_trabajadores_temporarios Otras:".$id_trabtemp_otras_id."\n");
		}
		/* mano_de_obra_completa */
		$ins_sql = " insert into mano_de_obra_completa (version, completa_id, parcial_id, contratados_preparacion_suelo_id, contratados_siembra_id, 
contratados_culturales_id, contratados_cosecha_id, contratados_otras_id, contratados_otra_tarea, 
integrantes_completo, integrantes_parcial, permanentes_contratados_completo, permanentes_contratados_parcial) ";
		$ins_sql .= " values (0, ".
			$id_trabtemp_completa_id.", ".
			$id_trabtemp_parcial_id.", ".
			$id_trabtemp_preparacion_id.", ".
			$id_trabtemp_siembra_id.", ".
			$id_trabtemp_labores_id.", ".
			$id_trabtemp_cosecha_id.", ".
			$id_trabtemp_otras_id.", '".
			$row_tit['OtrasTexto']."' ,".
			$row_tit['TrabFamPermanentesCompleto'].", ".
			$row_tit['TrabFamPermanentesParcial'].", ".
			$row_tit['TrabPermanentesCompleta'].", ".
			$row_tit['TrabPermanentesParcial']." )";
		echoif("mano_de_obra_completas\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT mano_de_obra_completa failed: ");
			$Errores['mano_de_obra_completa']++;
			echoif("\n\n");
		} else {    
			$id_mano_de_obra_completa = $Tconn->insert_id;
			echoif(" id mano_de_obra_completa:".$id_mano_de_obra_completa."\n");
		}

		  /***********************/						
		 /* contrata_maquinaria */
		/***********************/
		$ins_sql = " insert into contrata_maquinaria (version, contrata_maquinaria, contratados_cosecha, contratados_labores_culturales, contratados_otra_tarea, 
contratados_otras, contratados_preparacion_suelo, contratados_siembra, dias_contratados_cosecha, 
dias_contratados_labores_culturales, dias_contratados_otras, dias_contratados_preparacion_suelo, dias_contratados_siembra) ";
		$ins_sql .= " values (0, ".
			$row_tit['Maquinaria'].", ".
			$row_tit['ServCosechaPersonas'].", ".
			$row_tit['ServCulturalesPersonas'].", '".
			$row_tit['ServOtraTexto']."', ".
			$row_tit['ServOtraPersonas'].", ".
			$row_tit['ServPreparacionPersonas'].", ".
			$row_tit['ServSiembraPersonas'].", ".
			$row_tit['ServCosechaDias'].", ".
			$row_tit['ServCulturalesDias'].", ".
			$row_tit['ServOtraDias'].", ".
			$row_tit['ServPreparacionDias'].", ".
			$row_tit['ServSiembraDias']." )";
		echoif("contrata_maquinaria\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT contrata_maquinaria failed: ");
			$Errores['contrata_maquinaria']++;
			echoif("\n\n");
		} else {    
			$id_contrata_maquinaria = $Tconn->insert_id;
			echoif(" id mano_de_obra_completa:".$id_contrata_maquinaria."\n");
		}

		  /********************/						
		 /* vivienda_detalle */
		/********************/
		$ins_sql = " insert into vivienda_detalle (version,
						dormitorios,
						las_paredes_exteriores_tienen_revoqueorevestimiento,
						material_predomiante_de_las_paredes,
						material_predomiante_de_los_pisos,
						material_predomiante_del_techo,
						otro_material_predomiante_de_las_paredes,
						otro_material_predomiante_de_los_pisos,
						otro_material_predomiante_del_techo,
						otros_combustibles,
						tiene_acceso_vehicular_transitable_todo_el_anio,
						tiene_agua_corriente,
						tiene_agua_corriente_dentro_de_la_casa,
						tiene_banio_instalado,
						tiene_cocina_instalado,
						tiene_otro_tipo_de_desague,
						tiene_red_cloacal,
						tiene_red_electrica,
						tiene_ruta_cercanaavivienda,
						usan_gas_envasado,
						usan_gas_natural,
						usan_lena ) ";
		$ins_sql .= " values (0, ".
			$row_tit['VIVDormitorios'].", ".
			$row_tit['SBIRevoque'].", '".
			$row_tit['paredes_desc']."', '".
			$row_tit['piso_desc']."', '".
			$row_tit['techo_desc']."', ".
			"'', ".  /* FALTA! otros materiales */
			"'', ".
			"'', '".
			$row_tit['SBIOtrosTexto']."' ,".
			$row_tit['SBICaminos'].", ".
			$row_tit['SBIAguaRed'].", ".
			$row_tit['SBIAguaAdentro'].", ".
			$row_tit['VIVBanio'].", ".
			$row_tit['VIVCocina'].", ".
			$row_tit['SBDesague'].", ".
			$row_tit['SBCloaca'].", ".
			$row_tit['SBILuz'].", ".
			$row_tit['SBIRedVial'].", ".
			$row_tit['SBIGasEnvasado'].", ".
			$row_tit['SBIGas'].", ".
			$row_tit['SBILenia']." )";
		echoif("vivienda_detalle\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT vivienda_detalle failed: ");
			$Errores['vivienda_detalle']++;
			echoif("\n\n");
		} else {    
			$id_vivienda_detalle = $Tconn->insert_id;
			echoif(" id vivienda_detalle:".$id_vivienda_detalle."\n");
		}
		
		/* vivienda_detalle_agua_de_otros_origenes */
		$lista="";
		if ($row_tit['SBIperforacion']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'perforacion')";
		}
		if ($row_tit['SBIpozo']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'pozo')";
		}

		if ($row_tit['SBIlluvia']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'lluvia')";
		}

		if ($row_tit['SBIcisterna']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'cisterna')";
		}

		if ($row_tit['SBIrio']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'rio')";
		}

		if ($row_tit['SBIcanal']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'canal')";
		}

		if ($row_tit['SBIarroyo']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'arroyo')";
		}
				
		if ($row_tit['SBIotro']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'Otro origen (SBIotro)')";
		}
		if ($row_tit['SBIAguaOtro']) {
			if (!empty($lista)) { $lista .= ", "; }
			$lista .= "(".$id_vivienda_detalle.", 'Otro origen (SBIAguaOtro)')";
		}
		if (!empty($lista)) { 
	 		$ins_sql = " insert into vivienda_detalle_agua_de_otros_origenes (vivienda_detalle_id, agua_de_otros_origenes_string) ";
			$ins_sql .= " values ".$lista;
			echoif("vivienda_detalle_agua_de_otros_origenes 2\n");
			echoif($ins_sql."\n");
			if (!$Tconn->query($ins_sql)) {
				pdberror($Tconn, $ins_sql."\n"."INSERT vivienda_detalle_agua_de_otros_origenes failed: ");
				$Errores['vivienda_detalle_agua_de_otros_origenes']++;
				echoif("\n\n");
			}
		}
		
		  /*****************/						
		 /* salud_detalle */
		/*****************/
		/* emergencia */
		/* centro_saludosalita */ 
		if ($row_tit['SALUDDispensarioSiNo'] == 1)  {
			$id_superficie = ins_superficie($Tconn, $row_tit['SALUDDispensario'], $row_tit['SALUDDispernsarioUni']);
			$ins_sql = " insert into emergencia (version, asiste, distanciaavivienda_id) ";
			$ins_sql .= " values (0, 1, ".$id_superficie." )";
			echoif("emergencia centro_saludosalita\n");
			echoif($ins_sql."\n");
			if (!$Tconn->query($ins_sql)) {
				pdberror($Tconn, $ins_sql."\n"."INSERT emergencia centro_saludosalita failed: ");
				$Errores['emergencia']++;
				echoif("\n\n");
			} else {    
				$id_centro_saludosalita = $Tconn->insert_id;
				echoif(" id emergencia centro_saludosalita :".$id_centro_saludosalita."\n");
			}
		} else {
			$id_centro_saludosalita = "null";			
		}
		/* clinicaosanatorio_prepagaoparticular */
		if ($row_tit['SALUDClinicaPrepSiNo'] == 1) { 
			$id_superficie = ins_superficie($Tconn, $row_tit['SALUDClinicaPrep'], $row_tit['SALUDClinicaPrepUni']);
			$ins_sql = " insert into emergencia (version, asiste, distanciaavivienda_id) ";
			$ins_sql .= " values (0, 1, ".$id_superficie." )";
			echoif("emergencia clinicaosanatorio_prepagaoparticular\n");
			echoif($ins_sql."\n");
			if (!$Tconn->query($ins_sql)) {
				pdberror($Tconn, $ins_sql."\n"."INSERT emergencia clinicaosanatorio_prepagaoparticular failed: ");
				$Errores['emergencia']++;
				echoif("\n\n");
			} else {    
				$id_clinicaosanatorio_prepagaoparticular = $Tconn->insert_id;
				echoif(" id emergencia clinicaosanatorio_prepagaoparticular :".$id_clinicaosanatorio_prepagaoparticular."\n");
			}
		} else {
			$id_clinicaosanatorio_prepagaoparticular = "null";			
		}		
		
		/* clinicaosanatorio_obra_social */
		if ($row_tit['SALUDClinicaOSSiNo'] == 1) { 
			$id_superficie = ins_superficie($Tconn, $row_tit['SALUDClinicaOS'], $row_tit['SALUDClinicaOSUni']);
			$ins_sql = " insert into emergencia (version, asiste, distanciaavivienda_id) ";
			$ins_sql .= " values (0, 1, ".$id_superficie." )";
			echoif("emergencia clinicaosanatorio_obra_social\n");
			echoif($ins_sql."\n");
			if (!$Tconn->query($ins_sql)) {
				pdberror($Tconn, $ins_sql."\n"."INSERT emergencia clinicaosanatorio_obra_social failed: ");
				$Errores['emergencia']++;
				echoif("\n\n");
			} else {    
				$id_clinicaosanatorio_obra_social = $Tconn->insert_id;
				echoif(" id emergencia clinicaosanatorio_obra_social :".$id_clinicaosanatorio_obra_social."\n");
			}
		} else {
			$id_clinicaosanatorio_obra_social = "null";			
		}
		
		/* hospital */
		if ($row_tit['SALUDHospitalSiNo'] == 1) { 
			$id_superficie = ins_superficie($Tconn, $row_tit['SALUDHospital'], $row_tit['SALUDHospitalUni']);
			$ins_sql = " insert into emergencia (version, asiste, distanciaavivienda_id) ";
			$ins_sql .= " values (0, 1, ".$id_superficie." )";
			echoif("emergencia hospital\n");
			echoif($ins_sql."\n");
			if (!$Tconn->query($ins_sql)) {
				pdberror($Tconn, $ins_sql."\n"."INSERT emergencia hospital failed: ");
				$Errores['emergencia']++;
				echoif("\n\n");
			} else {    
				$id_hospital = $Tconn->insert_id;
				echoif(" id emergencia hospital :".$id_hospital."\n");
			}
		} else {
			$id_hospital = "null";			
		}
		
		$ins_sql = " insert into salud_detalle (version
			, centro_saludosalita_id
			, clinicaosanatorio_obra_social_id
			, clinicaosanatorio_prepagaoparticular_id
			, cobertura_obra_social
			, cobertura_prepaga
			, cobertura_publica
			, hospital_id
			, sin_cobertura) ";
		$ins_sql .= " values (0, ".$id_centro_saludosalita.", ".$id_clinicaosanatorio_obra_social.", ".$id_clinicaosanatorio_prepagaoparticular.", "
			.$row_tit['SaludCoberturaOsocial'].", ".$row_tit['SaludCoberturaPrePaga'].", ".$row_tit['SaludCoberturaEstatal'].", ".$id_hospital.", ".$row_tit['SaludSinCobertura']." )";
		echoif("salud_detalle\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT salud_detalle failed: ");
			$Errores['salud_detalle']++;
			echoif("\n\n");
		} else {    
			$id_salud_detalle = $Tconn->insert_id;
			echoif(" id salud_detalle :".$id_salud_detalle."\n");
		}
				
		  /********************************/						
		 /* distancia_viviendaaeducacion */
		/********************************/
		/* escuelaEducacionEspecial */
		$id_superficie = ins_superficie($Tconn, $row_tit['EDUCEducEspecial'], $row_tit['EDUCEducEspecialUni']);
		$ins_sql = " insert into distancia_viviendaaeducacion (version, centro_educativo, distancia_id) ";
		$ins_sql .= " values (0, 'escuelaEducacionEspecial', ".$id_superficie." )";
		echoif("distancia_viviendaaeducacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT distancia_viviendaaeducacion failed: ");
			$Errores['distancia_viviendaaeducacion']++;
			echoif("\n\n");
		} else {    
			$id_escuelaEducacionEspecial = $Tconn->insert_id;
			echoif(" id distancia_viviendaaeducacion :".$id_escuelaEducacionEspecial."\n");
		}
		/* escuelaPrimaria */
		$id_superficie = ins_superficie($Tconn, $row_tit['EDUCPrimario'], $row_tit['EDUCPrimarioUni']);
		$ins_sql = " insert into distancia_viviendaaeducacion (version, centro_educativo, distancia_id) ";
		$ins_sql .= " values (0, 'escuelaPrimaria', ".$id_superficie." )";
		echoif("distancia_viviendaaeducacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT distancia_viviendaaeducacion failed: ");
			$Errores['distancia_viviendaaeducacion']++;
			echoif("\n\n");
		} else {    
			$id_escuelaPrimaria = $Tconn->insert_id;
			echoif(" id distancia_viviendaaeducacion :".$id_escuelaPrimaria."\n");
		}
		/* escuelaSecundaria */
		$id_superficie = ins_superficie($Tconn, $row_tit['EDUCSecundario'], $row_tit['EDUCSecundarioUni']);
		$ins_sql = " insert into distancia_viviendaaeducacion (version, centro_educativo, distancia_id) ";
		$ins_sql .= " values (0, 'escuelaSecundaria', ".$id_superficie." )";
		echoif("distancia_viviendaaeducacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT distancia_viviendaaeducacion failed: ");
			$Errores['distancia_viviendaaeducacion']++;
			echoif("\n\n");
		} else {    
			$id_escuelaSecundaria = $Tconn->insert_id;
			echoif(" id distancia_viviendaaeducacion :".$id_escuelaSecundaria."\n");
		}
		/* escuelaTerciaria */
		$id_superficie = ins_superficie($Tconn, $row_tit['EDUCTerciario'], $row_tit['EDUCTerciarioUni']);
		$ins_sql = " insert into distancia_viviendaaeducacion (version, centro_educativo, distancia_id) ";
		$ins_sql .= " values (0, 'escuelaTerciaria', ".$id_superficie." )";
		echoif("distancia_viviendaaeducacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT distancia_viviendaaeducacion failed: ");
			$Errores['distancia_viviendaaeducacion']++;
			echoif("\n\n");
		} else {    
			$id_escuelaTerciaria = $Tconn->insert_id;
			echoif(" id distancia_viviendaaeducacion :".$id_escuelaTerciaria."\n");
		}
		/* guarderia */
		$id_superficie = ins_superficie($Tconn, $row_tit['EDUCGuarderia'], $row_tit['EDUCGuarderiaUni']);
		$ins_sql = " insert into distancia_viviendaaeducacion (version, centro_educativo, distancia_id) ";
		$ins_sql .= " values (0, 'guarderia', ".$id_superficie." )";
		echoif("distancia_viviendaaeducacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT distancia_viviendaaeducacion failed: ");
			$Errores['distancia_viviendaaeducacion']++;
			echoif("\n\n");
		} else {    
			$id_guarderia = $Tconn->insert_id;
			echoif(" id distancia_viviendaaeducacion :".$id_guarderia."\n");
		}
		/* jardinDeInfantes */
		$id_superficie = ins_superficie($Tconn, $row_tit['EDUCJardin'], $row_tit['EDUCJardinUni']);
		$ins_sql = " insert into distancia_viviendaaeducacion (version, centro_educativo, distancia_id) ";
		$ins_sql .= " values (0, 'jardinDeInfantes', ".$id_superficie." )";
		echoif("distancia_viviendaaeducacion\n");
		echoif($ins_sql."\n");
		if (!$Tconn->query($ins_sql)) {
			pdberror($Tconn, $ins_sql."\n"."INSERT distancia_viviendaaeducacion failed: ");
			$Errores['distancia_viviendaaeducacion']++;
			echoif("\n\n");
		} else {    
			$id_jardinDeInfantes = $Tconn->insert_id;
			echoif(" id distancia_viviendaaeducacion :".$id_jardinDeInfantes."\n");
		}
	
				
		  /**************************/						
		 /* Actualizo naf_completo */
		/**************************/
		$upd_nafcompleto = " update naf_completo ";
		$upd_nafcompleto .= " set actividad_id = ".$id_actividad_completa;
		$upd_nafcompleto .= " , centros_salud_id = ".$id_salud_detalle;
		$upd_nafcompleto .= " , contrata_maquinaria_id = ".$id_contrata_maquinaria;
		$upd_nafcompleto .= " , distanciaavivienda = ".$row_tierra['DistanciaAlPredio'];
		$upd_nafcompleto .= " , domicilio_id = ".$id_domicilio;
		$upd_nafcompleto .= " , domicilio_produccion_id = ".$id_domicilio_prod;
		$upd_nafcompleto .= " , escuela_educacion_especial_id = ".$id_escuelaEducacionEspecial;
		$upd_nafcompleto .= " , escuela_primaria_id = ".$id_escuelaPrimaria;
		$upd_nafcompleto .= " , escuela_secundaria_id = ".$id_escuelaSecundaria;
		$upd_nafcompleto .= " , escuela_terciaria_id = ".$id_escuelaTerciaria;
		$upd_nafcompleto .= " , familia_administra = ".$row_tit['Duenios'];
		$upd_nafcompleto .= " , familia_decide_donde_se_vende = ".$row_tit['DecidenVender'];
		$upd_nafcompleto .= " , guarderia_id = ".$id_guarderia;
		$upd_nafcompleto .= " , jardin_de_infantes_id = ".$id_jardinDeInfantes;
		$upd_nafcompleto .= " , mano_de_obra_id = ".$id_mano_de_obra_completa;
		$upd_nafcompleto .= " , recursos_id = ".$id_recursos;
		$upd_nafcompleto .= " , tecnologia_id = ".$id_tecnologia;
//tiene_croquis
		$upd_nafcompleto .= " , tierra_id = ".$id_tierra;
		$upd_nafcompleto .= " , unidad_distanciaavivienda = '".$row_tierra['DistanciaAlPredioUnidad']."'";
		$upd_nafcompleto .= " , vivienda_detalle_id = ".$id_vivienda_detalle;
//date_created
//fecha_creacion
//last_updated
//observaciones
//telefono_contacto
		$upd_nafcompleto .= " where id = ".$id_naf_completo;
		
		if (!$Tconn->query($upd_nafcompleto)) {
			pdberror($Tconn, $upd_nafcompleto."\n"."UPDATE naf_completo (domicilios) failed: ");
			$Errores['naf_completo']++;
			echoif("\n\n");
		}		

		$Tconn->commit();
		echoif("\n\n\n");

	}
}
echo "FIN!\n";
print_r($Errores);

function ins_superficie ($conn, $medida, $unidad_id) {
	$id_superficie_completa = -1;
	$ins_superficie_completa = " insert into superficie_completa (version, medida, unidad_id) ";
	$ins_superficie_completa .= " values (0, ".$medida.", ".$unidad_id." )";
	echoif("superficie_completa\n");
	echoif($ins_superficie_completa."\n");
	if (!$conn->query($ins_superficie_completa)) {
		pdberror($conn, $ins_superficie_completa."\nINSERT superficie_completa failed: ");
		$Errores['superficie_completa']++;
		echoif("\n\n");
	} else {    
		$id_superficie_completa = $conn->insert_id;
		echoif(" id superficie_completa:".$id_superficie_completa."\n");
	}
	return $id_superficie_completa;
}
function unidad_infraestructura($codigo) {
	
	return $unidad;
}
function vaciar_tablas($conn, $listaTablas) {
	$conn->query("SET foreign_key_checks = 0;");
	echoif("borrando tablas...\n");
	foreach ($listaTablas as $tabla) {
		$del_query = "delete FROM ".$tabla.";"; 
		echoif($del_query." ");
		if (!$conn->query($del_query)) {
			pdberror($conn, $del_query." failed: ");
			echoif("\n\n");
		} else {
			echoif("Se borro ok ".$tabla."\n");
		}
	}
	$conn->query("SET foreign_key_checks = 1;");
	echoif("Listo borrar tablas\n\n");
	return;
}

function ins_familiares($Sconn, $Tconn, $id_naf_completo, $k1, $k2, $k3) {
	$sel_fam  = " select NumeroDocumento, Apellido, Nombres, TipoDocumento, ";
	$sel_fam .= " Parentesco, CyParentesco, FechaNacimiento, NivelEducativo, EnElPredio, TrabajaEnPredio, asiste ";	
	$sel_fam .= " tipodoc.id td_id, tipodoc.codigo td_codigo, tipodoc.abrev td_abrev, tipodoc.descripcion td_descripcion, ";
	$sel_fam .= " ifnull(niveled.id, 0) niveled_id ";
	$sel_fam .= " from familiares "; 
	$sel_fam .= " left join ".TARGETDB.".tipo_documento tipodoc on ifnull(TipoDocumento, '-1') = tipodoc.codigo ";
	$sel_fam .= " left join ".TARGETDB.".nivel_educativo niveled on ifnull(NivelEducativo, '0') = niveled.codigo ";
	$sel_fam .= " where VentanillaRegistro = ".$k1." and pc = ".$k2." and CorrelativoTitular = ".$k3;
	$res_fam = $Sconn->query($sel_fam);
	$nRows_fam = $Sconn->affected_rows;
	if ($nRows_fam > 0) {
		$i=0;
		while ($row_fam = mysqli_fetch_array($res_fam)) {
			/* persona */
			$apellido = mysqli_real_escape_string($Tconn, trim($row_fam['Apellido']));
			$nombres = mysqli_real_escape_string($Tconn, trim($row_fam['Nombres']));
			$ins_persona = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_persona .= " VALUES ( 0, '".$apellido."', '".trim($row_fam['NumeroDocumento'])."', '".trim($row_fam['FechaNacimiento']);
			$ins_persona .= "', 'Argentina', '".$nombres."', 'M', ".$row_fam['NumeroDocumento'].")";
			echoif($ins_persona."\n");
			if (!$Tconn->query($ins_persona)) {
				pdberror($Tconn, "INSERT persona (familiar ".$i.") failed: ");
				$Errores['persona']++;
				echoif("\n\n");
			} else {
				$id_persona = $Tconn->insert_id;
				echoif(" id persona (familiar):".$id_persona."\n\n");
			}
			/* integrante */
			$ins_integrante = " INSERT INTO integrante (version, nivel_educativo_id, parentescoh_id, parentescom_id, persona_id, trabaja_en_naf) ";
			$ins_integrante .= " VALUES ( 0, ".trim($row_fam['niveled_id']).", ".trim($row_fam['Parentesco']).", ".trim($row_tit['CyParentesco']).", ".$id_persona.", b'".$row_fam['TrabajaEnPredio']."')";
			echoif($ins_integrante."\n");
			if (!$Tconn->query($ins_integrante)) {
				pdberror($Tconn, "INSERT integrante (familiar) failed: ");
				$Errores['integrante']++;
				echoif("\n\n");
			} else {
				$id_integrante = $Tconn->insert_id;
				echoif(" id integrante (familiar):".$id_integrante."\n\n");
			}
			/* naf_completo_integrante */
			$ins_naf_completo_integrante  = " insert into naf_completo_integrante (naf_completo_integrantes_id, integrante_id) ";
			$ins_naf_completo_integrante .= " values (".$id_naf_completo.", ".$id_integrante.")";
			echoif("naf_completo_integrante (familiar)\n");
			echoif($ins_naf_completo_integrante."\n");
			if (!$Tconn->query($ins_naf_completo_integrante)) {
				pdberror($Tconn, "INSERT naf_completo_integrante (familiar) failed: ");
				$Errores['naf_completo_integrante']++;
				echoif("\n\n");
			}		
			
									
		}
	}
}
function ins_explotacion_con_limite($conn, $limite, $condicion, $unidad, $condicionEnum) {

	$id_superficie = ins_superficie($conn, $condicion, $unidad);

	$ins_explotacion_con_limites = " insert into explotacion_con_limites (version, condicion, otra, superficie_id) ";
	if (!$condicionEnum == "OTRA") {
		$ins_explotacion_con_limites .= " values ( 0, '".$condicionEnum."', null, ".$id_superficie." )";
	} else {
		$ins_explotacion_con_limites .= " values ( 0, null, '".$condicionEnum."', ".$id_superficie." )";
	}
	echoif("explotacion_con_limites \n");
	echoif($ins_explotacion_con_limites."\n");
	if (!$conn->query($ins_explotacion_con_limites)) {
		pdberror($conn, $ins_explotacion_con_limites."\n"."INSERT explotacion_con_limites failed: ");
		$Errores['explotacion_con_limites']++;
		echoif("\n\n");
	} else {    
		$id_explotacion_con_limites = $conn->insert_id;
		echoif(" id explotacion_con_limites :".$id_explotacion_con_limites."\n");
	}

	$ins_limite_explotacion_con_limites = " insert into limite_explotacion_con_limites (limite_explotaciones_con_limites_id, explotacion_con_limites_id) ";
	$ins_limite_explotacion_con_limites .= " values (".$limite.", ".$id_explotacion_con_limites." )";
	echoif("limite_explotacion_con_limites \n");
	echoif($ins_limite_explotacion_con_limites."\n");
	if (!$conn->query($ins_limite_explotacion_con_limites)) {
		pdberror($conn, $ins_limite_explotacion_con_limites."\n"."INSERT limite_explotacion_con_limites failed: ");
		$Errores['limite_explotacion_con_limites']++;
		echoif("\n\n");
	} 

	return 1;
}
function ins_explotacion_sin_limite($conn, $limite, $condicion, $cco, $prn, $otf, $otr) {

	$ins_explotacion_sin_limites = " insert into explotacion_sin_limites (version, campo_comunero, condicion, otras, otras_tierras_fiscales, parqueoreserva_natural) ";
	$ins_explotacion_sin_limites .= " values ( 0, ".$cco.", '".$condicion."', ".$otr.", ".$otf.", ".$prn." )";
	echoif("explotacion_sin_limites \n");
	echoif($ins_explotacion_sin_limites."\n");
	if (!$conn->query($ins_explotacion_sin_limites)) {
		pdberror($conn, $ins_explotacion_sin_limites."\n"."INSERT explotacion_sin_limites failed: ");
		$Errores['explotacion_sin_limites']++;
		echoif("\n\n");
	} else {    
		$id_explotacion_sin_limites = $conn->insert_id;
		echoif(" id explotacion_sin_limites :".$id_explotacion_sin_limites."\n");
	}

	$ins_limite_explotacion_sin_limites = " insert into limite_explotacion_sin_limites (limite_explotaciones_sin_limites_id, explotacion_sin_limites_id) ";
	$ins_limite_explotacion_sin_limites .= " values (".$limite.", ".$id_explotacion_sin_limites." )";
	echoif("limite_explotacion_sin_limites \n");
	echoif($ins_limite_explotacion_sin_limites."\n");
	if (!$conn->query($ins_limite_explotacion_sin_limites)) {
		pdberror($conn, $ins_limite_explotacion_sin_limites."\n"."INSERT limite_explotacion_sin_limites failed: ");
		$Errores['limite_explotacion_sin_limites']++;
		echoif("\n\n");
	} 

	return 1;
}

function ins_tipo_juridico($conn, $id_tierra, $descripcion, $tipo) {
	/* tipo_juridico */
	$ins_tipo_juridico = " insert into tipo_juridico (version, descripcion, tipo )";
	$ins_tipo_juridico .= " values (0, '".mysqli_real_escape_string($conn, $descripcion)."', '".$tipo."' )";
	echoif("tipo_juridico\n");
	echoif($ins_tipo_juridico."\n");
	if (!$conn->query($ins_tipo_juridico)) {https://www.google.com.ar/
		pdberror($conn, $ins_tipo_juridico."\n"."INSERT tipo_juridico failed: ");$Errores['tipo_juridico']++;echoif("\n\n");
	} else {    
		$id_tipo_juridico = $conn->insert_id;
		echoif(" id tipo_juridico :".$id_tipo_juridico."\n");
	}
	/* tierra_tipo_juridico */
	$ins_tierra_tipo_juridico = " insert into tierra_tipo_juridico (tierra_tipos_juridicos_id, tipo_juridico_id)";
	$ins_tierra_tipo_juridico .= " values (".$id_tierra.", ".$id_tipo_juridico." )";
	echoif("tierra_tipo_juridico\n");
	echoif($ins_tierra_tipo_juridico."\n");
	if (!$conn->query($ins_tierra_tipo_juridico)) {
		pdberror($conn, $ins_tierra_tipo_juridico."\n"."INSERT tierra_tipo_juridico failed: ");$Errores['tierra_tipo_juridico']++;echoif("\n\n");
	}
	
	return 1;
	
}

function ins_riesgo($conn, $id_tierra, $cod, $descripcion, $frec) {
	/* riesgo */
	$ins_riesgo = " insert into riesgo (version, codigo, descripcion, frecuencia )";
	$ins_riesgo .= " values (0, '".$cod."', '".$descripcion."', '".$frec."' )";
	echoif("riesgo\n");
	echoif($ins_riesgo."\n");
	if (!$conn->query($ins_riesgo)) {
		pdberror($conn, $ins_riesgo."\n"."INSERT riesgo failed: ");$Errores['riesgo']++;echoif("\n\n");
	} else {    
		$id_riesgo = $conn->insert_id;
		echoif(" id riesgo :".$id_riesgo."\n");
	}
	/* tierra_riesgo */
	$ins_tierra_riesgo = " insert into tierra_riesgo (tierra_riesgos_id, riesgo_id )";
	$ins_tierra_riesgo .= " values (".$id_tierra.", ".$id_riesgo." )";
	echoif("tierra_riesgo\n");
	echoif($ins_tierra_riesgo."\n");
	if (!$conn->query($ins_tierra_riesgo)) {
		pdberror($conn, $ins_tierra_riesgo."\n"."INSERT tierra_riesgo failed: ");$Errores['tierra_riesgo']++;echoif("\n\n");
	}
	
	return 1;
	
}
function ins_prevencion ($conn, $id_tierra, $adoptado, $cod, $descripcion ){
	/* tipo_juridico */
	$ins_prevencion = " insert into prevencion (version, adoptado, codigo, descripcion )";
	$ins_prevencion .= " values (0, ".$adoptado.", '".$cod."', '".$descripcion."' )";
	echoif("prevencion\n");
	echoif($ins_prevencion."\n");
	if (!$conn->query($ins_prevencion)) {
		pdberror($conn, $ins_prevencion."\n"."INSERT prevencion failed: ");$Errores['prevencion']++;echoif("\n\n");
	} else {    
		$id_prevencion = $conn->insert_id;
		echoif(" id prevencion :".$id_prevencion."\n");
	}
	/* tierra_tipo_juridico */
	$ins_tierra_prevencion = " insert into tierra_prevencion (tierra_prevenciones_id, prevencion_id )";
	$ins_tierra_prevencion .= " values (".$id_tierra.", ".$id_prevencion." )";
	echoif("tierra_prevencion\n");
	echoif($ins_tierra_prevencion."\n");
	if (!$conn->query($ins_tierra_prevencion)) {
		pdberror($conn, $ins_tierra_prevencion."\n"."INSERT tierra_prevencion failed: ");$Errores['tierra_prevencion']++;echoif("\n\n");
	}
	
}
?>