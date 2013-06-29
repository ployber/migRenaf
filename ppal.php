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

//		echo $i." ".$tit_IdDocumento." ".$tit_TipoDocumento." - ".$tit_NumeroDocumento."\n";

		/* INSERT persona */
		if ($tit_NumeroDocumento != "") {
			$ins_titular = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_titular .= " VALUES ( 0, '".$tit_Apellido."', '".$tit_NumeroDocumento."', '".$tit_FechaNacimiento."', 'Argentina', '".$tit_Nombres."', 'M', ".$tit_IdDocumento.")";
			echo $ins_titular."\n";
			if (!$Tconn->query($ins_titular)) {
				echo "INSERT persona (titular) failed: (" . $Tconn->errno . ") " . $Tconn->error;
			}    
			$id_titular = $Tconn->insert_id;
			echo "id persona:".$id_titular."\n";
		}

		if ($cy_NumeroDocumento != "") {
			$ins_conyuge = " INSERT INTO persona (version, apellido, documento, fecha_nacimiento, nacionalidad, nombre, sexo, tipo_documento_id) ";
			$ins_conyuge .= " VALUES ( 0, '".$cy_Apellido."', '".$cy_NumeroDocumento."', '".$cy_FechaNacimiento."', 'Argentina', '".$cy_Nombres."', 'F', ".$cy_IdDocumento.")";
			echo $ins_conyuge."\n";
			if (!$Tconn->query($ins_conyuge)) {
				echo "INSERT persona (conyuge) failed: (" . $Tconn->errno . ") " . $Tconn->error;
			}    
			$id_conyuge = $Tconn->insert_id;
			echo "id conyuge:".$id_conyuge."\n";
			//$ins_result = Tdb_query($ins_conyuge);
		}
 		/* INSERT titular_completa */
 		$ins_tit_completa = " INSERT INTO titular_completa (version, persona_id) ";
		$ins_tit_completa .= " VALUES ( 0, '".$id_titular."' )";
		if (!$Tconn->query($ins_tit_completa)) {
			echo "INSERT titular_completa failed: (" . $Tconn->errno . ") " . $Tconn->error;
		}    
		$id_tit_completa = $Tconn->insert_id;
		echo "id titular_completa:".$id_conyuge."\n";
		
		/* INSERT naf_completo */
		$ins_naf_completo = " insert into naf () ";
		$ins_naf_completo .= " values ()";
				
/*
	
		NumeroDocumento
		FechaNacimiento
		Apellido
		Nombres
		"M"
		*/
		$Tconn->commit();

		$i++;
		if ($i > $tope) {
			$result->close();
		}
				
	}
}
echo "FIN!"
?>