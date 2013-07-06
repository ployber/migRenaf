<?php
include("include/db_funcs.php"); 
include("include/funciones.php");

define("DEBUG",0);
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
	"act_pastoreo_detalle_sub_producto_animal"=>0
);

$listaTablas = Array(
				"act_agricultura_act_agricultura_detalle",
				"act_agricultura_detalle",   
				"act_agroindustria_act_agroindustria_detalle",
				"act_agroindustria_detalle",
				"act_apicultura_detalle",   
				"act_artesania_detalle",   
				"act_apicultura_act_apicultura_detalle",
				"act_artesania_act_artesania_detalle",
				"act_caza_act_caza_detalle",
				"act_caza_detalle",   
				"act_pesca_act_pesca_detalle",
				"act_pesca_detalle",   
				"act_recoleccion_detalle",   
				"act_turismo_rural_detalle",   
				"act_recoleccion_act_recoleccion_detalle",
				"act_turismo_rural_act_turismo_rural_detalle",
				"sub_producto_animal",   
				"act_pastoreo_detalle_sub_producto_animal",
				"act_pastoreo_detalle",   
				"act_pastoreo_act_pastoreo_detalle",
				"actividad_completa_actividad_complementaria",
				"actividad_complementaria",  
				"actividad_completa_actividad_principal",
				"actividad_principal",   
				"actividad_completa_adicionales",
				"mejora_produccion",
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
$TOPEFILASAPROCESAR=400;
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
echoif($nRows_titulares. "filas\n\n");
if ($nRows_titulares > 0) {
	$i=0;
	while ($row_tit = mysqli_fetch_array($res_titulares)) {
		//print_r($row_tit);
		$k1 = $row_tit['Ventanillaregistro']; 
		$k2 = $row_tit['pc']; 
		$k3 = $row_tit['Correlativo']; 
		
		$tit_TipoDocumento = $row_tit['TipoDocumento'];
		$tit_NumeroDocumento = $row_tit['NumeroDocumento'];
		$tit_Apellido = $row_tit['Apellido'];
		$tit_Nombres = $row_tit['Nombres'];
		$tit_FechaNacimiento = $row_tit['FechaNacimiento'];
		$tit_IdDocumento = $row_tit['tdtit_id'];

		$cy_TipoDocumento = $row_tit['CyTipoDocumento'];
		$cy_NumeroDocumento = $row_tit['CyNumeroDocumento'];
		$cy_Apellido = $row_tit['CyApellido'];
		$cy_Nombres = $row_tit['CyNombres'];
		$cy_FechaNacimiento = $row_tit['CyFechaNacimiento'];
		$cy_IdDocumento = $row_tit['tdcy_id'];
		
		if ($cy_NumeroDocumento != "" && $cy_NumeroDocumento != "0") {
			$SINCONYUGE=0;
		} else {
			$SINCONYUGE=1;
		}

		$sel_tierra = "select * from tierra where Ventanillaregistro = ".$k1." and pc = ".$k2." and Correlativo = ".$k3;
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

		$i++;
		if ($i > $TOPEFILASAPROCESAR) {
			//$result->close();
			break;
		}
		if (!empty($DESDE)) {
			if ($i < $DESDE -1) {
				continue;
			}
		}
		echoif("row ".$i." Key:[".$k1."-".$k2."-".$k3."]\n");
		if (($i % 100) == 0 ) {
			echo "row ".$i." Key:[".$k1."-".$k2."-".$k3."]\n";
		}
				
		$id_titular = -1;
		$id_conyuge = -1;
		$id_tit_completa_tit = -1;
		$id_tit_completa_cy = -1;
		
		/*
		naf_completo
		has many titular_completo
				 has persona
		has many integrante
				 has persona
		*/	 		
		/* INSERT persona */
		echoif("titular\n");
		echoif(" tit_IdDocumento[".$tit_IdDocumento."]\n tit_TipoDocumento[".$tit_TipoDocumento."]\n tit_NumeroDocumento[".$tit_NumeroDocumento."]\n tit_Nombres[".$tit_Nombres."]\n tit_Apellido[".$tit_Apellido."]\n");
		echoif(" Nivel educativo [".$row_tit['NivelEducativo']."]\n");
		echoif(" Nivel educativo Cy [".$row_tit['CyNivelEducativo']."]\n");
		echoif(" parentesco [".$row_tit['parentesco']."]\n");
		echoif(" Cyparentesco [".$row_tit['Cyparentesco']."]\n");
		
		if ($tit_NumeroDocumento != "") {
			$ins_titular = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_titular .= " VALUES ( 0, '".$tit_Apellido."', '".$tit_NumeroDocumento."', '".$tit_FechaNacimiento."', 'Argentina', '".$tit_Nombres."', 'M', ".$tit_IdDocumento.")";
			echoif($ins_titular."\n");
			if (!$Tconn->query($ins_titular)) {
				pdberror($Tconn, "INSERT persona (titular) failed: ");
				$Errores['persona']++;
				echoif("\n\n");
			} else {
				$id_titular = $Tconn->insert_id;
				echoif(" id persona:".$id_titular."\n\n");
			}
		} else {
			//salto a la fila siguiente
			echoif(" sin datos del titular, ignoro registro.\n\n");
			echoif("\n\n\n");
			continue;
		}
		if (empty($id_titular)) {
			$id_titular = "null";
		}

		echoif("conyuge\n");
		echoif(" cy_IdDocumento[".$cy_IdDocumento."]\n cy_TipoDocumento[".$cy_TipoDocumento."]\n cy_NumeroDocumento[".$cy_NumeroDocumento."]\n cy_Nombres[".$cy_Nombres."]\n cy_Apellido[".$cy_Apellido."]\n");
				
		if (!$SINCONYUGE) {
			$ins_conyuge = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_conyuge .= " VALUES ( 0, '".$cy_Apellido."', '".$cy_NumeroDocumento."', '".$cy_FechaNacimiento."', 'Argentina', '".$cy_Nombres."', 'F', ".$cy_IdDocumento.")";
			echoif($ins_conyuge."\n");
			if (!$Tconn->query($ins_conyuge)) {
				pdberror($Tconn, "INSERT persona (conyuge) failed: ");
				$Errores['persona']++;				
				echoif("\n\n");
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
		if ($tit_NumeroDocumento != "") {
			$ins_integrante = " INSERT INTO integrante (version, nivel_educativo_id, parentescoh_id, parentescom_id, persona_id, trabaja_en_naf) ";
			$ins_integrante .= " VALUES ( 0, ".trim($row_tit['niveled1_id']).", ".trim($row_tit['parentesco1_id']).", ".trim($row_tit['parentesco2_id']).", ".$id_titular.", b'".$row_tit['TrabajaEnElPredio']."')";
			echoif($ins_integrante."\n");
			if (!$Tconn->query($ins_integrante)) {
				pdberror($Tconn, "INSERT integrante (titular) failed: ");
				$Errores['integrante']++;
				echoif("\n\n");
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
				pdberror($Tconn, "INSERT integrante (conyuge) failed: ");
				$Errores['integrante']++;
				echoif("\n\n");
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
			pdberror($Tconn, "INSERT titular_completa (titular) failed: ");
			$Errores['titular_completa']++;
			echoif("\n\n");
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
				pdberror($Tconn, "INSERT titular_completa (conyuge) failed: ");
				$Errores['titular_completa']++;
				echoif("\n\n");
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
			pdberror($Tconn, "INSERT naf_completo failed: ");
			$Errores['naf_completo']++;
			echoif("\n\n");
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
			pdberror($Tconn, "INSERT naf_completo_integrante failed: ");
			$Errores['naf_completo_integrante']++;
			echoif("\n\n");
		}
		
		/* resto integrantes - familiares */
		ins_familiares($Sconn, $Tconn, $id_naf_completo, $k1, $k2, $k3);

		/* domicilio_id */
		$ins_domicilio = "INSERT INTO domicilio (version, datos_catastrales, departamento_id, direccion, localidad, municipio_distrito_comuna_id, otra_referencia, paraje, provincia_id, codigo_postal) ";
		$ins_domicilio .= "VALUES ('0', "; 
		$ins_domicilio .= "'' ,"; /* datos_catastrales */ 
		$ins_domicilio .= $row_tierra['Departamento']." ,"; /* departamento_id */
		$ins_domicilio .= "'".$row_tierra['Domicilio']."' ,"; /* direccion */
		 /* geo_lat, geo_lat_renaf, geo_long, geo_long_renaf, */ 
		$ins_domicilio .= "'".$row_tierra['Localidad']."' ,"; /* localidad */
		$ins_domicilio .= $row_tierra['Municipio']." ,"; /* municipio_distrito_comuna_id */ 
		/* nrookm */
		$ins_domicilio .= "'".$row_tierra['OtraReferencia']."' ,"; /* otra_referencia */ 
		$ins_domicilio .= "'".$row_tierra['Paraje']."' ,"; /* paraje */
		$ins_domicilio .= $row_tierra['Provincia']." ,"; /* provincia_id */ 
		$ins_domicilio .= "'".$row_tierra['CP']."' "; /* codigo_postal */
		$ins_domicilio .= ")";
		echoif("Domicilio\n");
		echoif($ins_domicilio."\n");
		$id_domicilio = 0;
		if (!$Tconn->query($ins_domicilio)) {
			pdberror($Tconn, "INSERT domicilio (persona) failed: ");
			$Errores['domicilio']++;
			echoif("\n\n");
		} else {    
			$id_domicilio = $Tconn->insert_id;
			echoif(" id domicilio:".$id_domicilio."\n");
		}

		/* domicilio_produccion_id */
		$ins_domicilio = "INSERT INTO domicilio (version, datos_catastrales, departamento_id, direccion, localidad, municipio_distrito_comuna_id, otra_referencia, paraje, provincia_id, codigo_postal) ";
		$ins_domicilio .= "VALUES ('0', "; 
		$ins_domicilio .= "'".$row_tierra['EstabDatoCatastral']."' ,"; /* datos_catastrales */ 
		$ins_domicilio .= $row_tierra['EstabDepartamento']." ,"; /* departamento_id */
		$ins_domicilio .= "'".$row_tierra['EstabDomicilio']."' ,"; /* direccion */
		 /* geo_lat, geo_lat_renaf, geo_long, geo_long_renaf, */ 
		$ins_domicilio .= "'".$row_tierra['EstabLocalidad']."' ,"; /* localidad */
		$ins_domicilio .= $row_tierra['EstabMunicipio']." ,"; /* municipio_distrito_comuna_id */ 
		/* nrookm */
		$ins_domicilio .= "'".$row_tierra['EstabOtraReferencia']."' ,"; /* otra_referencia */ 
		$ins_domicilio .= "'".$row_tierra['EstabParaje']."' ,"; /* paraje */
		$ins_domicilio .= $row_tierra['EstabProvincia']." ,"; /* provincia_id */ 
		$ins_domicilio .= "'".$row_tierra['EstabCP']."' "; /* codigo_postal */
		$ins_domicilio .= ")";
		echoif("Domicilio Produccion\n");
		echoif($ins_domicilio."\n");
		$id_domicilio_prod = 0;
		if (!$Tconn->query($ins_domicilio)) {
			pdberror($Tconn, "INSERT domicilio (produccion) failed: ");
			$Errores['domicilio']++;
			echoif("\n\n");
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
		 * act_produccion ?
		 *
		 */ 

		/* actividad_completa */
		$ins_actividad_completa = " insert into actividad_completa (version, ingreso_adicional_anual, ingreso_complementario_anual) ";
		$ins_actividad_completa .= " values (0, 0, 0)";
		/* FALTA! ingreso_adicional_anual, ingreso_complementario_anual */
		echoif("actividad_completa\n");
		echoif($ins_actividad_completa."\n");
		if (!$Tconn->query($ins_actividad_completa)) {
			pdberror($Tconn, "INSERT actividad_completa failed: ");
			$Errores['actividad_completa']++;
			echoif("\n\n");
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
				pdberror($Tconn, "INSERT actividad_complementaria (alquilaTierra) failed: ");
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
				pdberror($Tconn, "UPDATE actividad_complementaria (superficie alquilaTierra) failed: ");
				$Errores['actividad_complementaria']++;
				echoif("\n\n");
			}

			/* actividad_completa_actividad_complementaria */
			$ins_actividad_completa_actividad_complementaria = " insert into actividad_completa_actividad_complementaria (actividad_completa_complementarias_id, actividad_complementaria_id) ";
			$ins_actividad_completa_actividad_complementaria .= " values (".$id_actividad_completa.", ".$id_actividad_complementaria.")";
			echoif("actividad_completa_actividad_complementaria (alquilaTierra)\n");
			echoif($ins_actividad_completa_actividad_complementaria."\n");
			if (!$Tconn->query($ins_actividad_completa_actividad_complementaria)) {
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (alquilaTierra) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServVtaFuerzaTrab) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServVtaFuerzaTrab) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServVtaFuerzaTrabEv) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServVtaFuerzaTrabEv) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServUsoANimales) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServUsoANimales) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServUsoMaquinarias) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServUsoMaquinarias) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServComercializacion) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServComercializacion) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServTransporte) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServTransporte) failed: ");
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
				pdberror($Tconn, "INSERT actividad_complementaria (ServTurismo) failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_complementaria (ServTurismo) failed: ");
				$Errores['actividad_completa_actividad_complementaria']++;
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
				pdberror($Tconn, "INSERT actividad_principal actPrincipalAgricultura failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalAgricultura failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_agricultura_detalle   rel   act_agricultura_act_agricultura_detalle */
			if ($nRows_pv > 0) {
				$pv=0;
				while ($row_pv = mysqli_fetch_array($res_pv)) {
					$pv++;
					
					/* act_produccion */
					$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
					$ins_act_produccion .= " values (0, '".trim($row_pv['can_cod'])."', ".$row_pv['PVAutoconsumo'].", ".$row_pv['PVMercado'].", ".$row_pv['PVIntercambio'].", ".$row_pv['PVPrecioUnitario'].", ".$row_pv['PVVolumen'].", ".$row_pv['PVUnidad'].", ".$row_pv['exp_cod']." )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, "INSERT act_produccion failed: ");$Errores['act_produccion']++;echoif("\n\n");
					} else {    
						$id_act_produccion = $Tconn->insert_id;
						echoif(" id act_produccion :".$id_act_produccion."\n");
					}
					/* superficie_completa */
					$id_superficie = ins_superficie($Tconn, $row_pv['PVACampo'], $row_pv['PVUn']);
					/* act_agricultura_detalle */
					$ins_act_agricultura_detalle = " insert into act_agricultura_detalle (version, bajo_cubierta, codigo, descripcion, produccion_id, superficie_id) ";
					$ins_act_agricultura_detalle .= " values (0, ".$row_pv['PVCubierta'].", ".$row_pv['act_cod'].", '".$row_pv['act_desc']."', ".$id_act_produccion.", ".$id_superficie.")";
					echoif("act_agricultura_detalle\n");
					echoif($ins_act_agricultura_detalle."\n");
					if (!$Tconn->query($ins_act_agricultura_detalle)) {
						pdberror($Tconn, "INSERT act_agricultura_detalle failed: ");
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
						pdberror($Tconn, "INSERT act_agricultura_act_agricultura_detalle failed: ");
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
				pdberror($Tconn, "INSERT actividad_principal actPrincipalAgroindustria failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalAgroindustria failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_agroindustria_detalle   rel   act_agroindustria_act_agroindustria_detalle */
			if ($nRows_ai > 0) {
				$ai=0;
				while ($row_ai = mysqli_fetch_array($res_ai)) {
					$ai++;
					
					/* act_produccion */
					$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
					$ins_act_produccion .= " values (0, '".trim($row_ai['can_cod'])."', ".$row_ai['AgroindustriaAutoconsumo'].", ".$row_ai['AgroindustriaMercado'].", ".$row_ai['AgroindustriaIntercambio'].", ".$row_ai['AgroindustriaPrecio'].", ".$row_ai['AgroindustriaVolumen'].", ".$row_ai['AgroindustriaUnidad'].", ".$row_ai['exp_cod']." )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, "INSERT act_produccion failed: ");$Errores['act_produccion']++;echoif("\n\n");
					} else {    
						$id_act_produccion = $Tconn->insert_id;
						echoif(" id act_produccion :".$id_act_produccion."\n");
					}
					/* act_agricultura_detalle */
					$ins_act_agroindustria_detalle = " insert into act_agroindustria_detalle (version, codigo, descripcion, produccion_id, produce_materia_prima) ";
					$ins_act_agroindustria_detalle .= " values (0, '".$row_ai['act_cod']."', '".$row_ai['act_desc']."', ".$id_act_produccion.", ".$row_ai['AgroindustriaProduceMP'].")";
					echoif("act_agroindustria_detalle\n");
					echoif($ins_act_agroindustria_detalle."\n");
					if (!$Tconn->query($ins_act_agroindustria_detalle)) {
						pdberror($Tconn, "INSERT act_agroindustria_detalle failed: ");
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
						pdberror($Tconn, "INSERT act_agroindustria_act_agroindustria_detalle failed: ");
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
				pdberror($Tconn, "INSERT actividad_principal actPrincipalApicultura failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalApicultura failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* act_apicultura_detalle   rel   act_apicultura_act_apicultura_detalle */
			/* MIEL */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaMielCanal'])."', ".$row_tit['PAApiculturaMielAutoConsumo'].", ".$row_tit['PAApiculturaMielMercado'].", ".$row_tit['PAApiculturaMielIntercambio'].", ".$row_tit['PAApiculturaMielPrecioUnitario'].", ".$row_tit['PAApiculturaMielVolumen'].", ".$row_tit['PAApiculturaMielUnidades'].", ".$row_tit['PAApiculturaMielExplotacion']." )";
			echoif("act_produccion MIEL\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion MIEL failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle MIEL:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* CERA */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaCeraCanal'])."', ".$row_tit['PAApiculturaCeraAutoConsumo'].", ".$row_tit['PAApiculturaCeraMercado'].", ".$row_tit['PAApiculturaCeraIntercambio'].", ".$row_tit['PAApiculturaCeraPrecioUnitario'].", ".$row_tit['PAApiculturaCeraVolumen'].", ".$row_tit['PAApiculturaCeraUnidades'].", ".$row_tit['PAApiculturaCeraExplotacion']." )";
			echoif("act_produccion CERA\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion CERA failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle CERA:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
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
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaPropoleoCanal'])."', ".$row_tit['PAApiculturaPropoleoAutoConsumo'].", ".$row_tit['PAApiculturaPropoleoMercado'].", ".$row_tit['PAApiculturaPropoleoIntercambio'].", ".$row_tit['PAApiculturaPropoleoPrecioUnitario'].", ".$row_tit['PAApiculturaPropoleoVolumen'].", ".$row_tit['PAApiculturaPropoleoUnidades'].", ".$row_tit['PAApiculturaPropoleoExplotacion']." )";
			echoif("act_produccion PROPOLEO\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion PROPOLEO failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle PROPOLEO:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
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
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaJaleaCanal'])."', ".$row_tit['PAApiculturaJaleaAutoConsumo'].", ".$row_tit['PAApiculturaJaleaMercado'].", ".$row_tit['PAApiculturaJaleaIntercambio'].", ".$row_tit['PAApiculturaJaleaPrecioUnitario'].", ".$row_tit['PAApiculturaJaleaVolumen'].", ".$row_tit['PAApiculturaJaleaUnidades'].", ".$row_tit['PAApiculturaJaleaExplotacion']." )";
			echoif("act_produccion JALEA\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion JALEA failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle JALEA:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
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
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaPolenCanal'])."', ".$row_tit['PAApiculturaPolenAutoConsumo'].", ".$row_tit['PAApiculturaPolenMercado'].", ".$row_tit['PAApiculturaPolenIntercambio'].", ".$row_tit['PAApiculturaPolenPrecioUnitario'].", ".$row_tit['PAApiculturaPolenVolumen'].", ".$row_tit['PAApiculturaPolenUnidades'].", ".$row_tit['PAApiculturaPolenExplotacion']." )";
			echoif("act_produccion POLEN\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion POLEN failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle POLEN:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
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
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaNucleosCanal'])."', ".$row_tit['PAApiculturaNucleosAutoConsumo'].", ".$row_tit['PAApiculturaNucleosMercado'].", ".$row_tit['PAApiculturaNucleosIntercambio'].", ".$row_tit['PAApiculturaNucleosPrecioUnitario'].", ".$row_tit['PAApiculturaNucleosVolumen'].", ".$row_tit['PAApiculturaNucleosUnidades'].", ".$row_tit['PAApiculturaNucleosExplotacion']." )";
			echoif("act_produccion NUCLEOS\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion NUCLEOS failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle NUCLEOS:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
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
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaMaterialVivoCanal'])."', ".$row_tit['PAApiculturaMaterialVivoAutoConsumo'].", ".$row_tit['PAApiculturaMaterialVivoMercado'].", ".$row_tit['PAApiculturaMaterialVivoIntercambio'].", ".$row_tit['PAApiculturaMaterialVivoPrecioUnitario'].", ".$row_tit['PAApiculturaMaterialVivoVolumen'].", ".$row_tit['PAApiculturaMaterialVivoUnidades'].", ".$row_tit['PAApiculturaMaterialVivoExplotacion']." )";
			echoif("act_produccion REINAS\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion REINAS failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle REINAS:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
			}
			/* OTRO */
			/* act_produccion */
			$ins_act_produccion = " insert into act_produccion (version, canal_venta, destino_auto_consumo, destino_mercado, destino_trueque, precio, produccion_anual_cantidad, produccion_anual_unidad_id, tipo_explotacion )"; 
			$ins_act_produccion .= " values (0, '".trim($row_tit['PAApiculturaOtroCanal'])."', ".$row_tit['PAApiculturaOtroAutoConsumo'].", ".$row_tit['PAApiculturaOtroMercado'].", ".$row_tit['PAApiculturaOtroIntercambio'].", ".$row_tit['PAApiculturaOtroPrecioUnitario'].", ".$row_tit['PAApiculturaOtroVolumen'].", ".$row_tit['PAApiculturaOtroUnidades'].", ".$row_tit['PAApiculturaOtroExplotacion']." )";
			echoif("act_produccion OTRO\n");
			echoif($ins_act_produccion."\n");
			if (!$Tconn->query($ins_act_produccion)) {
				pdberror($Tconn, "INSERT act_produccion OTRO failed: ");$Errores['act_produccion']++;echoif("\n\n");
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
				pdberror($Tconn, "INSERT act_apicultura_detalle failed: ");
				$Errores['act_apicultura_detalle']++;
				echoif("\n\n");
			} else {    
				$id_act_apicultura_detalle = $Tconn->insert_id;
				echoif(" id act_apicultura_detalle OTRO:".$id_act_apicultura_detalle."\n");
			}
			/* act_apicultura_act_apicultura_detalle */
			$ins_act_apicultura_act_apicultura_detalle = " insert into act_apicultura_act_apicultura_detalle (act_apicultura_detalles_id, act_apicultura_detalle_id) ";
			$ins_act_apicultura_act_apicultura_detalle .= " values (".$id_actividad_principal.", ".$id_act_agroindustria_detalle." )";
			echoif("act_apicultura_act_apicultura_detalle\n");
			echoif($ins_act_apicultura_act_apicultura_detalle."\n");
			if (!$Tconn->query($ins_act_apicultura_act_apicultura_detalle)) {
				pdberror($Tconn, "INSERT act_apicultura_act_apicultura_detalle failed: ");
				$Errores['act_apicultura_act_apicultura_detalle']++;
				echoif("\n\n");
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
				pdberror($Tconn, "INSERT actividad_principal actPrincipalArtesania failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalArtesania failed: ");
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
					$ins_act_produccion .= " values (0, '".trim($row_ar['can_cod'])."', null, null, null, ".$row_ar['ArtesaniaPrecio'].", ".$row_ar['ArtesaniaVolumen'].", ".$row_ar['ArtesaniaUnidad'].", ".$row_ar['exp_cod']." )";
					echoif("act_produccion\n");
					echoif($ins_act_produccion."\n");
					if (!$Tconn->query($ins_act_produccion)) {
						pdberror($Tconn, "INSERT act_produccion failed: ");$Errores['act_produccion']++;echoif("\n\n");
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

		/* actividad_principal ACT_PRINCIPAL_CAZA = 'actPrincipalCaza', 'renaf.ActCaza' */
		if ($row_tit['ProdCaza']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalCaza', 'renaf.ActCaza', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalCaza\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, "INSERT actividad_principal actPrincipalCaza failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalCaza failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* FALTA!				act_caza_detalle   rel   act_caza_act_caza_detalle */
		}

		/* actividad_principal ACT_PRINCIPAL_PESCA = 'actPrincipalPesca', 'renaf.ActPesca' */
		if ($row_tit['ProdPesca']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalPesca', 'renaf.ActPesca', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalPesca\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, "INSERT actividad_principal actPrincipalPesca failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalPesca failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* FALTA!				act_pesca_detalle   rel   act_pesca_act_pesca_detalle */
		}

		/* actividad_principal ACT_PRINCIPAL_RECOLECCION = 'actPrincipalRecoleccion', 'renaf.ActRecoleccion' */
		if ($row_tit['ProdRecoleccion']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalRecoleccion', 'renaf.ActRecoleccion', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalRecoleccion\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, "INSERT actividad_principal actPrincipalRecoleccion failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalRecoleccion failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* FALTA!				act_recoleccion_detalle   rel   act_recoleccion_act_recoleccion_detalle */
		}

		/* actividad_principal ACT_PRINCIPAL_TURISMO_RURAL = 'actPrincipalTurismoRural', 'renaf.ActTurismoRural' */
		if ($row_tit['ProdTirismoRural']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalTurismoRural', 'renaf.ActTurismoRural', null, null, 0, 0)";
			echoif("actividad_principal actPrincipalTurismoRural\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, "INSERT actividad_principal actPrincipalTurismoRural failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalTurismoRural failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* FALTA!				act_turismo_rural_detalle   rel   act_turismo_rural_act_turismo_rural_detalle */
		}

		/* actividad_principal ACT_PRINCIPAL_PASTOREO = 'actPrincipalPastoreo', 'renaf.ActPastoreo' */
		if ($row_tit['ProdAnimal']) {
			$ins_actividad_principal = " insert into actividad_principal (version, descripcion, class, cantidad_colmenas_propias, cantidad_colmenas_terceros, certificacion_organica, produccion_organica) ";
			$ins_actividad_principal .= " values (0, 'actPrincipalPastoreo', 'renaf.ActPastoreo', null, null, ".$row_tit['ProdAnimalOrganicaCertificada'].", ".$row_tit['ProdAnimalOrganica'].")";
			echoif("actividad_principal actPrincipalPastoreo\n");
			echoif($ins_actividad_principal."\n");
			if (!$Tconn->query($ins_actividad_principal)) {
				pdberror($Tconn, "INSERT actividad_principal actPrincipalPastoreo failed: ");
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
				pdberror($Tconn, "INSERT actividad_completa_actividad_principal actPrincipalPastoreo failed: ");
				$Errores['actividad_completa_actividad_principal']++;
				echoif("\n\n");
			}
			/* FALTA!		act_pastoreo_detalle   rel   act_pastoreo_act_pastoreo_detalle
			 * 					sub_producto_animal   rel   act_pastoreo_detalle_sub_producto_animal
			 */
		}	
		
		/* TODAS */
		/* 7   actividad_principal ACT_PRINCIPAL_AGRICULTURA = 'actPrincipalAgricultura', 'renaf.ActAgricultura */
		/* 8.a actividad_principal ACT_PRINCIPAL_PASTOREO = 'actPrincipalPastoreo', 'renaf.ActPastoreo' */
		/* 8.d actividad_principal ACT_PRINCIPAL_APICULTURA = 'actPrincipalApicultura', 'renaf.ActApicultura' */
		/* 9   actividad_principal ACT_PRINCIPAL_ARTESANIA = 'actPrincipalArtesania', 'renaf.ActArtesania' */
		/* 10  actividad_principal ACT_PRINCIPAL_AGROINDUSTRIA = 'actPrincipalAgroindustria', 'renaf.ActAgroindustria' */
		/* 12  actividad_principal ACT_PRINCIPAL_CAZA = 'actPrincipalCaza', 'renaf.ActCaza' */
		/* 13  actividad_principal ACT_PRINCIPAL_PESCA = 'actPrincipalPesca', 'renaf.ActPesca' */
		/* 11  actividad_principal ACT_PRINCIPAL_RECOLECCION = 'actPrincipalRecoleccion', 'renaf.ActRecoleccion' */
		/* 14  actividad_principal ACT_PRINCIPAL_TURISMO_RURAL = 'actPrincipalTurismoRural', 'renaf.ActTurismoRural' */
												 
		/*
		 * 			has many
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
		 */
		
		/* Actualizo naf_completo */
		$upd_nafcompleto = " update naf_completo ";
		$upd_nafcompleto .= " set domicilio_id = ".$id_domicilio;
		$upd_nafcompleto .= " , actividad_id = ".$id_actividad_completa;
		$upd_nafcompleto .= " , domicilio_produccion_id = ".$id_domicilio_prod;
		$upd_nafcompleto .= " , distanciaavivienda = ".$row_tierra['DistanciaAlPredio'];
		$upd_nafcompleto .= " , unidad_distanciaavivienda = '".$row_tierra['DistanciaAlPredioUnidad']."'";
		$upd_nafcompleto .= " where id = ".$id_naf_completo;
		
		if (!$Tconn->query($upd_nafcompleto)) {
			pdberror($Tconn, "UPDATE naf_completo (domicilios) failed: ");
			$Errores['naf_completo']++;
			echoif("\n\n");
		}		
		 	


/* naf_completo		
, centros_salud_id
, contrata_maquinaria_id
, escuela_educacion_especial_id
, escuela_primaria_id
, escuela_secundaria_id
, escuela_terciaria_id
, familia_administra
, familia_decide_donde_se_vende
, guarderia_id
, jardin_de_infantes_id
, mano_de_obra_id
, recursos_id
, tecnologia_id
, tiene_croquis
, tierra_id
, vivienda_detalle_id
, date_created
, fecha_creacion
, last_updated
, observaciones
, telefono_contacto
*/				
				
		$Tconn->commit();
		echoif("\n\n\n");

	}
}
echo "FIN!\n";
print_r($Errores);

function echoif($str) {
	if (! DEBUG) {
		echo $str;
	};
}
function pdberror($conn, $str) {
	$errors[] = $conn->error;
	echoif("=DB==================================================================\n");
    printf("error : %s\n", $Sconn->errno);
	print_r($errors);
	//echoif($str)." (" . $conn->errno . ") " . $conn->error."\n";
	echoif($str." (" . mysqli_errno($conn) . ") " . mysqli_error($conn)."\n");
	echoif("=====================================================================\n\n");
	}
function ins_superficie ($conn, $medida, $unidad_id) {
	$id_superficie_completa = -1;
	$ins_superficie_completa = " insert into superficie_completa (version, medida, unidad_id) ";
	$ins_superficie_completa .= " values (0, ".$medida.", ".$unidad_id." )";
	echoif("superficie_completa\n");
	echoif($ins_superficie_completa."\n");
	if (!$conn->query($ins_superficie_completa)) {
		pdberror($conn, "INSERT superficie_completa failed: ");
		$Errores['superficie_completa']++;
		echoif("\n\n");
	} else {    
		$id_superficie_completa = $conn->insert_id;
		echoif(" id superficie_completa:".$id_superficie_completa."\n");
	}
	return $id_superficie_completa;
}

function vaciar_tablas($conn, $listaTablas) {
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
			$ins_persona = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_persona .= " VALUES ( 0, '".trim($row_fam['Apellido'])."', '".trim($row_fam['NumeroDocumento'])."', '".trim($row_fam['FechaNacimiento']);
			$ins_persona .= "', 'Argentina', '".trim($row_fam['Nombres'])."', 'M', ".$row_fam['NumeroDocumento'].")";
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
?>