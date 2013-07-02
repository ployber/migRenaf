<?php
include("include/db_funcs.php"); 
include("include/funciones.php");

define("DEBUG",0);

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

$listaTablas = Array(
				"act_agricultura_detalle",   
				"act_agricultura_act_agricultura_detalle",
				"act_agroindustria_detalle",
				"act_agroindustria_act_agroindustria_detalle",
				"act_apicultura_detalle",   
				"act_apicultura_act_apicultura_detalle",
				"act_artesania_detalle",   
				"act_artesania_act_artesania_detalle",
				"act_caza_detalle",   
				"act_caza_act_caza_detalle",
				"act_pesca_detalle",   
				"act_pesca_act_pesca_detalle",
				"act_recoleccion_detalle",   
				"act_recoleccion_act_recoleccion_detalle",
				"act_turismo_rural_detalle",   
				"act_turismo_rural_act_turismo_rural_detalle",
				"sub_producto_animal",   
				"act_pastoreo_detalle_sub_producto_animal",
				"act_pastoreo_detalle",   
				"act_pastoreo_act_pastoreo_detalle",
				"actividad_completa_actividad_complementaria",
				"actividad_complementaria",  
				"actividad_principal",   
				"actividad_completa_actividad_principal",
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


vaciar_tablas($Tconn, $listaTablas);

$Tconn->commit();

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

$sel_titulares = select_titulares();

$res_titulares = $Sconn->query($sel_titulares); 
$nRows_titulares = $Sconn->affected_rows;
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
		$nRows_tierra = $Sconn->affected_rows;
		if ($nRows_tierra > 0) {
			$row_tierra = mysqli_fetch_array($res_tierra);		
		} else {
			$row_tierra = array();
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
 		
 		$ins_tit_completa = " INSERT INTO titular_completa (version, persona_id) ";
		$ins_tit_completa .= " VALUES ( 0, ".$id_titular." )";
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
	 		$ins_tit_completa = " INSERT INTO titular_completa (version, persona_id) ";
			$ins_tit_completa .= " VALUES ( 0, ".$id_conyuge." )";
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
				
		/* ACTIVIDADES */
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



/*		
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
	echoif("=DB==================================================================\n");
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
		echoif($del_query."\n");
		if (!$conn->query($del_query)) {
			pdberror($conn, $del_query." failed: ");
			echoif("\n\n");
		} else {
			echoif("Se borro ok ".$tabla."\n\n");
		}
	}
	return;
}

?>