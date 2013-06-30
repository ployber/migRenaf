<?php
include("include/db_funcs.php"); 
include("include/funciones.php");

$dbHostname = "localhost";
$dbUsername = "root";
$dbPassword = "feli0806";
$Sconn = new mysqli($dbHostname, $dbUsername, $dbPassword, SOURCEDB);
if (mysqli_connect_errno($Sconn)) {
  echo "Failed to connect to MySQL: " . mysqli_connect_error($Sconn);
} else {
	 echo "Connection OK con ".SOURCEDB."!\n";
}

$Tconn = new mysqli($dbHostname, $dbUsername, $dbPassword, TARGETDB);
if (mysqli_connect_errno($Tconn)) {
  echo "Failed to connect to MySQL: " . mysqli_connect_error($Tconn);
} else {
	 echo "Connection OK con ".TARGETDB."!\n";
}

$Tconn->autocommit(FALSE);

$result = $Tconn->query("delete FROM auditoria_planilla_reducida;");
$result = $Tconn->query("delete FROM planilla_reducida_action_alertado;");
$result = $Tconn->query("delete FROM transicionfid_planilla_reducida;");
$result = $Tconn->query("delete FROM planilla_reducida;");
$result = $Tconn->query("delete FROM naf_sector;");
$result = $Tconn->query("delete FROM naf;");
$result = $Tconn->query("delete FROM titular;");
$result = $Tconn->query("delete FROM titular_completa;");
$result = $Tconn->query("delete FROM persona;");

$Tconn->commit();

$sel_titulares = select_titulares();

$tope=10;
$result = $Sconn->query($sel_titulares); 
$numberOfRows = $Sconn->affected_rows;
if ($numberOfRows > 0) {
	$i=0;
	while ($row = mysqli_fetch_array($result)) {
		$tit_TipoDocumento = $row['TipoDocumento'];
		$tit_NumeroDocumento = $row['NumeroDocumento'];
		$tit_Apellido = $row['Apellido'];
		$tit_Nombres = $row['Nombres'];
		$tit_FechaNacimiento = $row['FechaNacimiento'];
		$tit_IdDocumento = $row['tdtit_id'];

		$cy_TipoDocumento = $row['CyTipoDocumento'];
		$cy_NumeroDocumento = $row['CyNumeroDocumento'];
		$cy_Apellido = $row['CyApellido'];
		$cy_Nombres = $row['CyNombres'];
		$cy_FechaNacimiento = $row['CyFechaNacimiento'];
		$cy_IdDocumento = $row['tdcy_id'];

		$i++;
		if ($i > $tope) {
			//$result->close();
			break;
		}
		echo "row ".$i."\n";
		
		$id_titular = -1;
		$id_conyuge = -1;
		$id_tit_completa_tit = -1;
		$id_tit_completa_cy = -1;
		
		/* INSERT persona */
		echo "titular\n";
		echo " tit_IdDocumento[".$tit_IdDocumento."]\n tit_TipoDocumento[".$tit_TipoDocumento."]\n tit_NumeroDocumento[".$tit_NumeroDocumento."]\n tit_Nombres[".$tit_Nombres."]\n tit_Apellido[".$tit_Apellido."]\n";
		if ($tit_NumeroDocumento != "") {
			$ins_titular = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_titular .= " VALUES ( 0, '".$tit_Apellido."', '".$tit_NumeroDocumento."', '".$tit_FechaNacimiento."', 'Argentina', '".$tit_Nombres."', 'M', ".$tit_IdDocumento.")";
			echo $ins_titular."\n";
			if (!$Tconn->query($ins_titular)) {
				echo "INSERT persona (titular) failed: (" . $Tconn->errno . ") " . $Tconn->error;
			} else {
				$id_titular = $Tconn->insert_id;
				echo " id persona:".$id_titular."\n\n";
			}
		} else {
			//salto a la fila siguiente
			echo " sin datos del titular, ignoro registro.\n\n";
			echo "\n\n\n";
			continue;
		}
		if (empty($id_titular)) {
			$id_titular = "null";
		}

		echo "conyuge\n";
		echo " cy_IdDocumento[".$cy_IdDocumento."]\n cy_TipoDocumento[".$cy_TipoDocumento."]\n cy_NumeroDocumento[".$cy_NumeroDocumento."]\n cy_Nombres[".$cy_Nombres."]\n cy_Apellido[".$cy_Apellido."]\n";		
		if ($cy_NumeroDocumento != "") {
			$ins_conyuge = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_conyuge .= " VALUES ( 0, '".$cy_Apellido."', '".$cy_NumeroDocumento."', '".$cy_FechaNacimiento."', 'Argentina', '".$cy_Nombres."', 'F', ".$cy_IdDocumento.")";
			echo $ins_conyuge."\n";
			if (!$Tconn->query($ins_conyuge)) {
				echo "INSERT persona (conyuge) failed: (" . $Tconn->errno . ") " . $Tconn->error;
			} else {    
				$id_conyuge = $Tconn->insert_id;
				echo " id conyuge:".$id_conyuge."\n";
			}
			//$ins_result = Tdb_query($ins_conyuge);
		}
		if (empty($id_conyuge)) {
			$id_conyuge = "null";
		}

 		/* INSERT titular_completa */
 		
 		$ins_tit_completa = " INSERT INTO titular_completa (version, persona_id) ";
		$ins_tit_completa .= " VALUES ( 0, ".$id_titular." )";
		echo "titular_completa (titular)\n";
		echo $ins_tit_completa."\n";
		if (!$Tconn->query($ins_tit_completa)) {
			echo "INSERT titular_completa (titular) failed: (" . $Tconn->errno . ") " . $Tconn->error;
		} else {
			$id_tit_completa_tit = $Tconn->insert_id;
			echo " id titular_completa (titular):".$id_tit_completa_tit."\n";
		}
		if ($cy_NumeroDocumento != "") {
	 		$ins_tit_completa = " INSERT INTO titular_completa (version, persona_id) ";
			$ins_tit_completa .= " VALUES ( 0, ".$id_conyuge." )";
			echo "titular_completa (conyuge)\n";
			echo $ins_tit_completa."\n";
			if (!$Tconn->query($ins_tit_completa)) {
				echo "INSERT titular_completa (conyuge) failed: (" . $Tconn->errno . ") " . $Tconn->error;
			}  else {
				$id_tit_completa_cy = $Tconn->insert_id;
				echo " id titular_completa (conyuge):".$id_tit_completa_cy."\n";
			}
		}
		
		$Tconn->commit();
		/* INSERT naf_completo */
		
		$ins_naf_completo = " insert into naf_completo (version, titular1_id, titular2_id ) ";
		if ($cy_NumeroDocumento != "") {
			$ins_naf_completo .= " values ('0', ".$id_tit_completa_tit.", ".$id_tit_completa_cy.")";
		} else {
			$ins_naf_completo .= " values ('0', ".$id_tit_completa_tit.", null)";
		}
		echo "naf_completo\n";
		echo $ins_naf_completo."\n";
		if (!$Tconn->query($ins_naf_completo)) {
			echo "INSERT naf_completo failed: (" . $Tconn->errno . ") " . $Tconn->error;
		} else {    
			$id_naf_completo = $Tconn->insert_id;
			echo " id naf_completo:".$id_naf_completo."\n";
		}




/*		
, actividad_id
, centros_salud_id
, contrata_maquinaria_id
, distanciaavivienda
, domicilio_id
, domicilio_produccion_id
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
		echo "\n\n\n";

	}
}
echo "FIN!"
?>