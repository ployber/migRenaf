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

$result = $Tconn->query("delete FROM auditoria_planilla_reducida;");
$result = $Tconn->query("delete FROM planilla_reducida_action_alertado;");
$result = $Tconn->query("delete FROM transicionfid_planilla_reducida;");
$result = $Tconn->query("delete FROM planilla_reducida;");
$result = $Tconn->query("delete FROM naf_sector;");
$result = $Tconn->query("delete FROM naf_completo_integrante;");
$result = $Tconn->query("delete FROM integrante;");
$result = $Tconn->query("delete FROM planilla_completa_action_alertado;");
$result = $Tconn->query("delete FROM transicionpr_planilla_completa;");
$result = $Tconn->query("delete FROM auditoria_planilla_completa;");
$result = $Tconn->query("delete FROM planilla_completa;");
$result = $Tconn->query("delete FROM naf_completo;");
$result = $Tconn->query("delete FROM titular;");
$result = $Tconn->query("delete FROM titular_completa;");
$result = $Tconn->query("delete FROM persona;");

$Tconn->commit();

$Errores = Array(
	"persona"=>0,
	"titular_completa"=>0,
	"integrante"=>0,
	"naf_completo"=>0,
	"domicilio"=>0
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
				pdberror($Tconn, "INSERT integrante (titular) failed: ");
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
				
		$upd_nafcompleto = " update naf_completo set domicilio_id = ".$id_domicilio.", domicilio_produccion_id=".$id_domicilio_prod;
		$upd_nafcompleto .= " where id = ".$id_naf_completo;
		if (!$Tconn->query($upd_nafcompleto)) {
			pdberror($Tconn, "UPDATE naf_completo (domicilios) failed: ");
			$Errores['naf_completo']++;
			echoif("\n\n");
		}		
				



/*		
, actividad_id
, centros_salud_id
, contrata_maquinaria_id
, distanciaavivienda
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
, unidad_distanciaavivienda
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

?>