<?php
define("SOURCEDB", "renaf_amigrar");
define("TARGETDB", "renaf_migrada");

function db_query($db_conn, $query) {
	if (!$result = $db_conn->query($query)) {
		die("Invalid query: " . $db_conn->error."<BR>".$query);	
	}
	return $result;
}
function echoif($str) {
	if (DEBUG) {
		echo $str;
	};
}
function pdberror($conn, $str) {
	$errors[] = $conn->error;
	echo "=DB==================================================================\n";
	echo "row:".$GLOBALS['i']." Key:[".$GLOBALS['k1']."-".$GLOBALS['k2']."-".$GLOBALS['k3']."]\n";

    printf("error : %s\n", $conn->errno);
	print_r($errors);
	//echoif($str)." (" . $conn->errno . ") " . $conn->error."\n";
	echo $str." (" . mysqli_errno($conn) . ") " . mysqli_error($conn)."\n";
	echo "=====================================================================\n\n";
	exit;
	}

function mostrarVariablesArray($arr) {echo $argc;
	
	echo '<table border="4"><tr><td>';
	echo '<br>';
	$arVal = array();
	while (list ($key, $val) = each ($arr)) {
		if ($val == "") $val = "NULL";
		$arVals[$key] = (get_magic_quotes_gpc()) ? $val : addslashes($val);
		echo $key . " : [" . $arVals[$key] . "]<br>";
	}	
	echo '<br>';
	echo '</td></tr></table>';
}
function select_titulares() {
	return "SELECT
titulares.Ventanillaregistro,
titulares.pc,
titulares.Correlativo,
titulares.formulario,
titulares.Confecciono,
titulares.ConfeccionadoPorApellido,
titulares.ConfeccionadoPorNombre,
titulares.Carga,
titulares.CargadoPor,
titulares.datosAl,
titulares.fechaCarga,
titulares.estado,
titulares.Observaciones,
titulares.Apellido,
titulares.Nombres,
titulares.TipoDocumento,
titulares.NumeroDocumento,
titulares.CuitCuil,
titulares.FechaNacimiento,
ifnull(titulares.NivelEducativo, 0), NivelEducativo, 
titulares.ViveEnElPredio,
ifnull(titulares.TrabajaEnElPredio, 0) TrabajaEnElPredio,
titulares.parentesco,
titulares.CyApellido,
titulares.CyNombres,
titulares.CyTipoDocumento,
titulares.CyNumeroDocumento,
titulares.CyCuitCuil,
titulares.CyFechaNacimiento,
ifnull(titulares.CyNivelEducativo, 0) CyNivelEducativo,
titulares.CyViveEnElPredio,
ifnull(titulares.CyTrabajaEnElPredio, 0) CyTrabajaEnElPredio,
titulares.Cyparentesco,
ifnull(titulares.ProdVegetal, 0) ProdVegetal,
ifnull(titulares.ProdAnimal, 0) ProdAnimal,
ifnull(titulares.ProdAgroindustria, 0) ProdAgroindustria,
ifnull(titulares.ProdArtesania, 0) ProdArtesania,
ifnull(titulares.ProdCaza, 0) ProdCaza,
ifnull(titulares.ProdRecoleccion, 0) ProdRecoleccion,
ifnull(titulares.ProdPesca, 0) ProdPesca,
ifnull(titulares.ProdTirismoRural, 0) ProdTirismoRural,
titulares.ForzadoProdVegetal,
titulares.ForzadoProdAnimal,
titulares.ForzadoProdAgroindustria,
titulares.ForzadoProdArtesania,
titulares.ForzadoProdPesca,
titulares.ForzadoEncabezado,
ifnull(titulares.ServAlqTierra, 0) ServAlqTierra,
ifnull(titulares.ServSuperficieAlquilada, 0) ServSuperficieAlquilada,
ifnull(titulares.ServSuperficieAlquiladaUnidades, 0) ServSuperficieAlquiladaUnidades,
ifnull(titulares.ServVtaFuerzaTrab, 0) ServVtaFuerzaTrab,
ifnull(titulares.ServVtaFuerzaTrabAgropecuario, 0) ServVtaFuerzaTrabAgropecuario,
ifnull(titulares.ServVtaFuerzaTrabEv, 0) ServVtaFuerzaTrabEv,
ifnull(titulares.ServVtaFuerzaTrabEvAgropecuario, 0) ServVtaFuerzaTrabEvAgropecuario,
ifnull(titulares.ServUsoANimales, 0) ServUsoANimales,
ifnull(titulares.ServUsoANimalesAgropecuario, 0) ServUsoANimalesAgropecuario,
ifnull(titulares.ServUsoMaquinarias, 0) ServUsoMaquinarias,
ifnull(titulares.ServUsoMaquinariasAgropecuario, 0) ServUsoMaquinariasAgropecuario,
ifnull(titulares.ServTurismo, 0) ServTurismo,
ifnull(titulares.ServTransporte, 0) ServTransporte,
ifnull(titulares.ServComercializacion, 0) ServComercializacion,
ifnull(titulares.ProdVegetalOrganica, 0) ProdVegetalOrganica,
ifnull(titulares.ProdVegetalOrganicaCertificada, 0) ProdVegetalOrganicaCertificada,  
ifnull(titulares.ProdAnimalOrganica, 0) ProdAnimalOrganica,
ifnull(titulares.ProdAnimalOrganicaCertificada, 0) ProdAnimalOrganicaCertificada,
ifnull(titulares.ProdAgrindustriaOrganica, 0) ProdAgrindustriaOrganica,
ifnull(titulares.ProdAgrindustriaOrganicaCertificada, 0) ProdAgrindustriaOrganicaCertificada,
ifnull(titulares.PRodCazaCarneAutoConsumo, 0) PRodCazaCarneAutoConsumo,
ifnull(titulares.PRodCazaCarneMercado, 0) PRodCazaCarneMercado,
ifnull(titulares.PRodCazaCarneIntercambio, 0) PRodCazaCarneIntercambio,
ifnull(titulares.PRodCazaCarneVolumen, 0) PRodCazaCarneVolumen,
ifnull(titulares.PRodCazaCarneUnidad, 0) PRodCazaCarneUnidad,
ifnull(titulares.PRodCazaCarnePrecio, 0) PRodCazaCarnePrecio,
ifnull(titulares.PRodCazaCarneExplotacion, '') PRodCazaCarneExplotacion,
ifnull(titulares.PRodCazaCuerosAutoConsumo, 0) PRodCazaCuerosAutoConsumo,
ifnull(titulares.PRodCazaCuerosMercado, 0) PRodCazaCuerosMercado,
ifnull(titulares.PRodCazaCuerosIntercambio, 0) PRodCazaCuerosIntercambio,
ifnull(titulares.PRodCazaCuerosVolumen, 0) PRodCazaCuerosVolumen,
ifnull(titulares.PRodCazaCuerosUnidad, 0) PRodCazaCuerosUnidad,
ifnull(titulares.PRodCazaCuerosPrecio, 0) PRodCazaCuerosPrecio,
ifnull(titulares.PRodCazaCuerosExplotacion, '') PRodCazaCuerosExplotacion,
ifnull(titulares.PRodCazaOtrosAutoConsumo, 0) PRodCazaOtrosAutoConsumo,
ifnull(titulares.PRodCazaOtrosMercado, 0) PRodCazaOtrosMercado,
ifnull(titulares.PRodCazaOtrosIntercambio, 0) PRodCazaOtrosIntercambio,
ifnull(titulares.PRodCazaOtrosVolumen, 0) PRodCazaOtrosVolumen,
ifnull(titulares.PRodCazaOtrosUnidad, 0) PRodCazaOtrosUnidad,
ifnull(titulares.PRodCazaOtrosPrecio, 0) PRodCazaOtrosPrecio,
ifnull(titulares.PRodCazaOtrosExplotacion, '') PRodCazaOtrosExplotacion,
ifnull(titulares.PRodRecoleccionVegetalesAutoConsumo, 0) PRodRecoleccionVegetalesAutoConsumo,
ifnull(titulares.PRodRecoleccionVegetalesMercado, 0) PRodRecoleccionVegetalesMercado,
ifnull(titulares.PRodRecoleccionVegetalesIntercambio, 0) PRodRecoleccionVegetalesIntercambio,
ifnull(titulares.PRodRecoleccionVegetalesVolumen, 0) PRodRecoleccionVegetalesVolumen,
ifnull(titulares.PRodRecoleccionVegetalesUnidad, 0) PRodRecoleccionVegetalesUnidad,
ifnull(titulares.PRodRecoleccionVegetalesPrecio, 0) PRodRecoleccionVegetalesPrecio,
ifnull(titulares.PRodRecoleccionVegetalesExplotacion, '') PRodRecoleccionVegetalesExplotacion,
ifnull(titulares.PRodRecoleccionMineralesAutoConsumo, 0) PRodRecoleccionMineralesAutoConsumo,
ifnull(titulares.PRodRecoleccionMineralesMercado, 0) PRodRecoleccionMineralesMercado,
ifnull(titulares.PRodRecoleccionMineralesIntercambio, 0) PRodRecoleccionMineralesIntercambio,
ifnull(titulares.PRodRecoleccionMineralesVolumen, 0) PRodRecoleccionMineralesVolumen,
ifnull(titulares.PRodRecoleccionMineralesUnidad, 0) PRodRecoleccionMineralesUnidad,
ifnull(titulares.PRodRecoleccionMineralesPrecio, 0) PRodRecoleccionMineralesPrecio,
ifnull(titulares.PRodRecoleccionMineralesExplotacion, '') PRodRecoleccionMineralesExplotacion,
titulares.PRodRecoleccionVegetalCanal,
titulares.PRodRecoleccionMineralCanal,
ifnull(titulares.PRodRecoleccionHongosAutoConsumo, 0) PRodRecoleccionHongosAutoConsumo,
ifnull(titulares.PRodRecoleccionHongosMercado, 0) PRodRecoleccionHongosMercado,
ifnull(titulares.PRodRecoleccionHongosIntercambio, 0) PRodRecoleccionHongosIntercambio,
ifnull(titulares.PRodRecoleccionHongosVolumen, 0) PRodRecoleccionHongosVolumen,
ifnull(titulares.PRodRecoleccionHongosUnidad, 0) PRodRecoleccionHongosUnidad,
ifnull(titulares.PRodRecoleccionHongosPrecio, 0) PRodRecoleccionHongosPrecio,
ifnull(titulares.PRodRecoleccionHongosExplotacion, '') PRodRecoleccionHongosExplotacion,
ifnull(titulares.PRodRecoleccionMielAutoConsumo, 0) PRodRecoleccionMielAutoConsumo,
ifnull(titulares.PRodRecoleccionMielMercado, 0) PRodRecoleccionMielMercado,
ifnull(titulares.PRodRecoleccionMielIntercambio, 0) PRodRecoleccionMielIntercambio,
ifnull(titulares.PRodRecoleccionMielVolumen, 0) PRodRecoleccionMielVolumen,
ifnull(titulares.PRodRecoleccionMielUnidad, 0) PRodRecoleccionMielUnidad,
ifnull(titulares.PRodRecoleccionMielPrecio, 0) PRodRecoleccionMielPrecio,
ifnull(titulares.PRodRecoleccionMielExplotacion, '') PRodRecoleccionMielExplotacion, 
ifnull(titulares.PRodRecoleccionOtroAutoConsumo, 0) PRodRecoleccionOtroAutoConsumo,
ifnull(titulares.PRodRecoleccionOtroMercado, 0) PRodRecoleccionOtroMercado,
ifnull(titulares.PRodRecoleccionOtroIntercambio, 0) PRodRecoleccionOtroIntercambio,
ifnull(titulares.PRodRecoleccionOtroVolumen, 0) PRodRecoleccionOtroVolumen,
ifnull(titulares.PRodRecoleccionOtroUnidad, 0) PRodRecoleccionOtroUnidad, 
ifnull(titulares.PRodRecoleccionOtroPrecio, 0) PRodRecoleccionOtroPrecio,
titulares.PRodRecoleccionOtroExplotacion,
titulares.PRodRecoleccionOtroTexto,
titulares.PRodCazaCarneCanal,
titulares.PRodCazaCuerosCanal,
titulares.PRodCazaOtrosCanal,
titulares.PRodCazaOtrosTexto,
ifnull(titulares.Riego, 0) Riego,
ifnull(titulares.RiegoSuperficie, 0) RiegoSuperficie,
ifnull(titulares.RiegoUnidades, 0) RiegoUnidades,
ifnull(titulares.RiegoTipo, 'x') RiegoTipo,
ifnull(titulares.AguaConsumoAnimal, 0) AguaConsumoAnimal,
ifnull(titulares.TraccionAnimal, 0) TraccionAnimal,
ifnull(titulares.TR1Cantidad, 0) TR1Cantidad,
ifnull(titulares.TR1Potencia, 0) TR1Potencia,
ifnull(titulares.TR1Modelo, '') TR1Modelo,
ifnull(titulares.TR1Adquisicion, 'x') TR1Adquisicion,
ifnull(titulares.TR1Propiedad, '') TR1Propiedad,
ifnull(titulares.TR2Cantidad, 0) TR2Cantidad,
ifnull(titulares.TR2Potencia, 0) TR2Potencia,
ifnull(titulares.TR2Modelo, '') TR2Modelo,
ifnull(titulares.TR2Adquisicion, 'x') TR2Adquisicion,
ifnull(titulares.TR2Propiedad, '') TR2Propiedad,
ifnull(titulares.VH1Cantidad, 0) VH1Cantidad,
ifnull(titulares.VH1Tipo, '') VH1Tipo,
ifnull(titulares.VH1Modelo, '') VH1Modelo,
ifnull(titulares.VH1Adquisicion, 'x') VH1Adquisicion,
ifnull(titulares.VH1Propiedad, '') VH1Propiedad,
ifnull(titulares.VH2Cantidad, 0) VH2Cantidad,
ifnull(titulares.VH2Tipo, '') VH2Tipo,
ifnull(titulares.VH2Modelo, '') VH2Modelo,
ifnull(titulares.VH2Adquisicion, 'x') VH2Adquisicion,
ifnull(titulares.VH2Propiedad, '') VH2Propiedad,
ifnull(titulares.PAApiculturaColmenasPropias, 0) PAApiculturaColmenasPropias,
ifnull(titulares.PAApiculturaColmenasTerceros, 0) PAApiculturaColmenasTerceros,
titulares.PAApiculturaMielColmenas,
titulares.PAApiculturaMielExplotacion,
titulares.PAApiculturaMielAutoConsumo,
titulares.PAApiculturaMielMercado,
titulares.PAApiculturaMielIntercambio,
titulares.PAApiculturaMielVolumen,
ifnull(titulares.PAApiculturaMielUnidades, 0) PAApiculturaMielUnidades,
ifnull(titulares.PAApiculturaMielPrecioUnitario, 0) PAApiculturaMielPrecioUnitario,
titulares.PAApiculturaCeraColmenas,
titulares.PAApiculturaCeraExplotacion,
titulares.PAApiculturaCeraAutoConsumo,
titulares.PAApiculturaCeraMercado,
titulares.PAApiculturaCeraIntercambio,
titulares.PAApiculturaCeraVolumen,
ifnull(titulares.PAApiculturaCeraUnidades, 0) PAApiculturaCeraUnidades,
ifnull(titulares.PAApiculturaCeraPrecioUnitario, 0) PAApiculturaCeraPrecioUnitario,
titulares.PAApiculturaPropoleoColmenas,
titulares.PAApiculturaPropoleoExplotacion,
titulares.PAApiculturaPropoleoAutoConsumo,
titulares.PAApiculturaPropoleoMercado,
titulares.PAApiculturaPropoleoIntercambio,
titulares.PAApiculturaPropoleoVolumen,
ifnull(titulares.PAApiculturaPropoleoUnidades, 0) PAApiculturaPropoleoUnidades,
ifnull(titulares.PAApiculturaPropoleoPrecioUnitario, 0) PAApiculturaPropoleoPrecioUnitario,
titulares.PAApiculturaJaleaColmenas,
titulares.PAApiculturaJaleaExplotacion,
titulares.PAApiculturaJaleaAutoConsumo,
titulares.PAApiculturaJaleaMercado,
titulares.PAApiculturaJaleaIntercambio,
titulares.PAApiculturaJaleaVolumen,
ifnull(titulares.PAApiculturaJaleaUnidades, 0) PAApiculturaJaleaUnidades,
ifnull(titulares.PAApiculturaJaleaPrecioUnitario, 0) PAApiculturaJaleaPrecioUnitario,
titulares.PAApiculturaPolenColmenas,
titulares.PAApiculturaPolenExplotacion,
titulares.PAApiculturaPolenAutoConsumo,
titulares.PAApiculturaPolenMercado,
titulares.PAApiculturaPolenIntercambio,
titulares.PAApiculturaPolenVolumen,
ifnull(titulares.PAApiculturaPolenUnidades, 0) PAApiculturaPolenUnidades,
ifnull(titulares.PAApiculturaPolenPrecioUnitario, 0) PAApiculturaPolenPrecioUnitario,
titulares.PAApiculturaNucleosColmenas,
titulares.PAApiculturaNucleosExplotacion,
titulares.PAApiculturaNucleosAutoConsumo,
titulares.PAApiculturaNucleosMercado,
titulares.PAApiculturaNucleosIntercambio,
titulares.PAApiculturaNucleosVolumen,
ifnull(titulares.PAApiculturaNucleosUnidades, 0) PAApiculturaNucleosUnidades,
ifnull(titulares.PAApiculturaNucleosPrecioUnitario, 0) PAApiculturaNucleosPrecioUnitario,
titulares.PAApiculturaMaterialVivoColmenas,
titulares.PAApiculturaMaterialVivoExplotacion,
titulares.PAApiculturaMaterialVivoAutoConsumo,
titulares.PAApiculturaMaterialVivoMercado,
titulares.PAApiculturaMaterialVivoIntercambio,
titulares.PAApiculturaMaterialVivoVolumen,
ifnull(titulares.PAApiculturaMaterialVivoUnidades, 0) PAApiculturaMaterialVivoUnidades, 
ifnull(titulares.PAApiculturaMaterialVivoPrecioUnitario, 0) PAApiculturaMaterialVivoPrecioUnitario,
titulares.PAApiculturaMielCanal,
titulares.PAApiculturaCeraCanal,
titulares.PAApiculturaPropoleoCanal,
titulares.PAApiculturaJaleaCanal,
titulares.PAApiculturaPolenCanal,
titulares.PAApiculturaNucleosCanal,
titulares.PAApiculturaMaterialVicoCanal,
titulares.PAApiculturaOtroCanal,
titulares.PAApiculturaOtroColmenas,
titulares.PAApiculturaOtroExplotacion,
titulares.PAApiculturaOtroAutoConsumo,
titulares.PAApiculturaOtroMercado,
titulares.PAApiculturaOtroIntercambio,
titulares.PAApiculturaOtroVolumen,
ifnull(titulares.PAApiculturaOtroUnidades, 0) PAApiculturaOtroUnidades,
ifnull(titulares.PAApiculturaOtroPrecioUnitario, 0) PAApiculturaOtroPrecioUnitario,
titulares.PAPescaCriaderoExplotacion,
ifnull(titulares.PAPescaCriaderoAutoConsumo, 0) PAPescaCriaderoAutoConsumo,
ifnull(titulares.PAPescaCriaderoMercado, 0) PAPescaCriaderoMercado,
ifnull(titulares.PAPescaCriaderoIntercambio, 0) PAPescaCriaderoIntercambio,
ifnull(titulares.PAPescaCriaderoVolumen, 0) PAPescaCriaderoVolumen,
ifnull(titulares.PAPescaCriaderoUnidad, 0) PAPescaCriaderoUnidad,
ifnull(titulares.PAPescaCriaderoPrecio, 0) PAPescaCriaderoPrecio, 
titulares.PAPescaRecoleccionExplotacion,
ifnull(titulares.PAPescaRecoleccionAutoConsumo, 0) PAPescaRecoleccionAutoConsumo,
ifnull(titulares.PAPescaRecoleccionMercado, 0) PAPescaRecoleccionMercado,
ifnull(titulares.PAPescaRecoleccionIntercambio, 0) PAPescaRecoleccionIntercambio,
ifnull(titulares.PAPescaRecoleccionVolumen, 0) PAPescaRecoleccionVolumen,
ifnull(titulares.PAPescaRecoleccionUnidad, 0) PAPescaRecoleccionUnidad,
ifnull(titulares.PAPescaRecoleccionPrecio, 0) PAPescaRecoleccionPrecio,
titulares.PAPescaCapturaExplotacion,
ifnull(titulares.PAPescaCapturaAutoConsumo, 0) PAPescaCapturaAutoConsumo,
ifnull(titulares.PAPescaCapturaMercado, 0) PAPescaCapturaMercado,
ifnull(titulares.PAPescaCapturaIntercambio, 0) PAPescaCapturaIntercambio,
ifnull(titulares.PAPescaCapturaVolumen, 0) PAPescaCapturaVolumen,
ifnull(titulares.PAPescaCapturaUnidad, 0) PAPescaCapturaUnidad,
ifnull(titulares.PAPescaCapturaPrecio, 0) PAPescaCapturaPrecio,
titulares.PAPescaCapturaCanal,
titulares.PAPescaCriaderoCanal,
titulares.PAPescaRecoleccionCanal,
titulares.PTRenPredio,
titulares.PTRenPredioExplotacion,
titulares.PTRenPredioIngresos,
titulares.PTRfueraPredio,
titulares.PTRFueraPredioExplotacion,
titulares.PTRfueraPredioIngresos,
ifnull(titulares.Duenios, 0) Duenios,
ifnull(titulares.DecidenVender, 0) DecidenVender,
ifnull(titulares.SemillasProduccioPropia, 0) SemillasProduccioPropia,
ifnull(titulares.SemillasCompra, 0) SemillasCompra,
ifnull(titulares.SemillasSubsudiado, 0) SemillasSubsudiado,
ifnull(titulares.AnimalesMejoraIA, 0) AnimalesMejoraIA,
ifnull(titulares.AnimalesMejoraSeleccion, 0) AnimalesMejoraSeleccion,
ifnull(titulares.AnimalesMejoraCruzamiento, 0) AnimalesMejoraCruzamiento,
ifnull(titulares.AnimalesMejoraOtra, 0) AnimalesMejoraOtra,
ifnull(titulares.AnimalesMejoraTexto, '') AnimalesMejoraTexto,
ifnull(titulares.AbonosOrganicosProduccion, 0) AbonosOrganicosProduccion,
ifnull(titulares.AbonosOrganicosCompra, 0) AbonosOrganicosCompra,
ifnull(titulares.AbonosOrganicosSubsidiado, 0) AbonosOrganicosSubsidiado,
ifnull(titulares.AbonosQuimicosProduccion, 0) AbonosQuimicosProduccion,
ifnull(titulares.AbonosQuimicosCompra, 0) AbonosQuimicosCompra,
ifnull(titulares.AbonosQuimicosSubsidiado, 0) AbonosQuimicosSubsidiado,
ifnull(titulares.ControlPlagaNoQuimicos, 0) ControlPlagaNoQuimicos,
ifnull(titulares.ControlPlagaBiologicos, 0) ControlPlagaBiologicos,
ifnull(titulares.ControlPlagaOtrosMetodos, 0) ControlPlagaOtrosMetodos,
ifnull(titulares.ControlPlagaQuimicos, 0) ControlPlagaQuimicos,
ifnull(titulares.Rotacion, 0) Rotacion,
ifnull(titulares.TrabPermanentesCompleta, 0) TrabPermanentesCompleta,
ifnull(titulares.TrabPermanentesParcial, 0) TrabPermanentesParcial,
ifnull(titulares.Preparacion1S,0) Preparacion1S,
ifnull(titulares.Preparacion1Q,0) Preparacion1Q,
ifnull(titulares.Preparacion1M,0) Preparacion1M,
ifnull(titulares.Preparacion2M,0) Preparacion2M,
ifnull(titulares.Preparacion3M,0) Preparacion3M,
ifnull(titulares.Preparacion4M,0) Preparacion4M,
ifnull(titulares.Preparacion5M,0) Preparacion5M,
ifnull(titulares.Preparacion6M,0) Preparacion6M,
ifnull(titulares.Siembra1S,0) Siembra1S,
ifnull(titulares.Siembra1Q,0) Siembra1Q,
ifnull(titulares.Siembra1M,0) Siembra1M,
ifnull(titulares.Siembra2M,0) Siembra2M,
ifnull(titulares.Siembra3M,0) Siembra3M,
ifnull(titulares.Siembra4M,0) Siembra4M,
ifnull(titulares.Siembra5M,0) Siembra5M,
ifnull(titulares.Siembra6M,0) Siembra6M,
ifnull(titulares.Labores1S,0) Labores1S,
ifnull(titulares.Labores1Q,0) Labores1Q,
ifnull(titulares.Labores1M,0) Labores1M,
ifnull(titulares.Labores2M,0) Labores2M,
ifnull(titulares.Labores3M,0) Labores3M,
ifnull(titulares.Labores4M,0) Labores4M,
ifnull(titulares.Labores5M,0) Labores5M,
ifnull(titulares.Labores6M,0) Labores6M,
ifnull(titulares.Cosecha1S,0) Cosecha1S,
ifnull(titulares.Cosecha1Q,0) Cosecha1Q,
ifnull(titulares.Cosecha1M,0) Cosecha1M,
ifnull(titulares.Cosecha2M,0) Cosecha2M,
ifnull(titulares.Cosecha3M,0) Cosecha3M,
ifnull(titulares.Cosecha4M,0) Cosecha4M,
ifnull(titulares.Cosecha5M,0) Cosecha5M,
ifnull(titulares.Cosecha6M,0) Cosecha6M,
ifnull(titulares.Otras1S,0) Otras1S,
ifnull(titulares.Otras1Q,0) Otras1Q,
ifnull(titulares.Otras1M,0) Otras1M,
ifnull(titulares.Otras2M,0) Otras2M,
ifnull(titulares.Otras3M,0) Otras3M,
ifnull(titulares.Otras4M,0) Otras4M,
ifnull(titulares.Otras5M,0) Otras5M,
ifnull(titulares.Otras6M,0) Otras6M,
titulares.OtrasTexto,
ifnull(titulares.Maquinaria, 0) Maquinaria,
titulares.ServPreparacionSup,
titulares.ServPreparacionUni,
ifnull(titulares.ServPreparacionPersonas, 0) ServPreparacionPersonas,
ifnull(titulares.ServPreparacionDias, 0) ServPreparacionDias,
titulares.ServSiembraSup,
titulares.ServSiembreUni,
ifnull(titulares.ServSiembraPersonas, 0) ServSiembraPersonas,
ifnull(titulares.ServSiembraDias, 0) ServSiembraDias,
titulares.ServCulturalesSup,
titulares.ServCulturalesUni,
ifnull(titulares.ServCulturalesPersonas, 0) ServCulturalesPersonas,
ifnull(titulares.ServCulturalesDias, 0) ServCulturalesDias,
titulares.ServCosechaSup,
titulares.ServCosechaUni,
ifnull(titulares.ServCosechaPersonas, 0) ServCosechaPersonas,
ifnull(titulares.ServCosechaDias, 0) ServCosechaDias,
titulares.ServOtraSup,
titulares.ServOtraUni,
ifnull(titulares.ServOtraPersonas, 0) ServOtraPersonas,
ifnull(titulares.ServOtraDias, 0) ServOtraDias,
titulares.ServOtraTexto,
titulares.Asesoramiento,
titulares.AsesoraminetoPaga,
titulares.AsesoramientoPrograma,
titulares.SBICentroProcCercano,
titulares.SBICentroProcUni,
titulares.SBIMercadoCercano,
titulares.SBIMercadoUni,
ifnull(titulares.SBIMaterialPisos, '') SBIMaterialPisos,
ifnull(titulares.SBIMaterialParedes, '') SBIMaterialParedes,
ifnull(titulares.SBIRevoque, 0) SBIRevoque,
ifnull(titulares.SBIMaterialTecho, '') SBIMaterialTecho,
ifnull(titulares.VIVDormitorios, 0) VIVDormitorios,
ifnull(titulares.VIVBanio, 0) VIVBanio,
ifnull(titulares.VIVCocina, 0) VIVCocina,
ifnull(titulares.SBCloaca, 0) SBCloaca,
ifnull(titulares.SBDesague, 0) SBDesague,
ifnull(titulares.SBIAguaRed, 0) SBIAguaRed,
ifnull(titulares.SBIAguaOtro, 0) SBIAguaOtro,
titulares.SBIAguaTipo,
ifnull(titulares.SBILuz, 0) SBILuz,
ifnull(titulares.SBIGas, 0) SBIGas,
ifnull(titulares.SBIGasEnvasado, 0) SBIGasEnvasado,
titulares.SBILenia,
titulares.SBIOtros,
ifnull(titulares.SBIOtrosTexto, '') SBIOtrosTexto, 
ifnull(titulares.SBIRedVial, 0) SBIRedVial,
ifnull(titulares.SBICaminos, 0) SBICaminos,
ifnull(titulares.SaludCoberturaOsocial, 0) SaludCoberturaOsocial,
ifnull(titulares.SaludCoberturaPrePaga, 0) SaludCoberturaPrePaga,
ifnull(titulares.SaludCoberturaEstatal, 0) SaludCoberturaEstatal,
ifnull(titulares.SaludSinCobertura, 0) SaludSinCobertura,
titulares.SALUDHospitalSiNo,
ifnull(titulares.SALUDHospital, 0) SALUDHospital,
ifnull(titulares.SALUDHospitalUni, 0) SALUDHospitalUni,
titulares.SALUDDispensarioSiNo,
ifnull(titulares.SALUDDispensario, 0) SALUDDispensario,
ifnull(titulares.SALUDDispernsarioUni, 0) SALUDDispernsarioUni,
titulares.SALUDClinicaOSSiNo,
ifnull(titulares.SALUDClinicaOS, 0) SALUDClinicaOS,
ifnull(titulares.SALUDClinicaOSUni, 0) SALUDClinicaOSUni,
titulares.SALUDClinicaPrepSiNo,
ifnull(titulares.SALUDClinicaPrep, 0) SALUDClinicaPrep,
ifnull(titulares.SALUDClinicaPrepUni, 0) SALUDClinicaPrepUni,
ifnull(titulares.EDUCGuarderia, 0) EDUCGuarderia,
ifnull(titulares.EDUCGuarderiaUni, 0) EDUCGuarderiaUni,
ifnull(titulares.EDUCJardin, 0) EDUCJardin,
ifnull(titulares.EDUCJardinUni, 0) EDUCJardinUni,
ifnull(titulares.EDUCPrimario, 0) EDUCPrimario,
ifnull(titulares.EDUCPrimarioUni, 0) EDUCPrimarioUni,
ifnull(titulares.EDUCSecundario, 0) EDUCSecundario,
ifnull(titulares.EDUCSecundarioUni, 0) EDUCSecundarioUni,
ifnull(titulares.EDUCTerciario, 0) EDUCTerciario,
ifnull(titulares.EDUCTerciarioUni, 0) EDUCTerciarioUni,
ifnull(titulares.EDUCEducEspecial, 0) EDUCEducEspecial,
ifnull(titulares.EDUCEducEspecialUni, 0) EDUCEducEspecialUni,
titulares.IngBrProduccion,
ifnull(titulares.IngBrServicios, 0) IngBrServicios,
ifnull(titulares.IngBrOtros, 0) IngBrOtros,
titulares.ComunidadOriginarios,
ifnull(titulares.TrabFamPermanentesParcial, 0) TrabFamPermanentesParcial,
ifnull(titulares.TrabFamPermanentesCompleto, 0) TrabFamPermanentesCompleto,
ifnull(titulares.TrabFamEventualParcial1S, 0) TrabFamEventualParcial1S,
ifnull(titulares.TrabFamEventualParcial1Q, 0) TrabFamEventualParcial1Q,
ifnull(titulares.TrabFamEventualParcial1M, 0) TrabFamEventualParcial1M,
ifnull(titulares.TrabFamEventualParcial2M, 0) TrabFamEventualParcial2M,
ifnull(titulares.TrabFamEventualParcial3M, 0) TrabFamEventualParcial3M,
ifnull(titulares.TrabFamEventualParcial4M, 0) TrabFamEventualParcial4M,
ifnull(titulares.TrabFamEventualParcial5M, 0) TrabFamEventualParcial5M,
ifnull(titulares.TrabFamEventualParcial6M, 0) TrabFamEventualParcial6M,
ifnull(titulares.TrabFamEventualCompleto1S, 0) TrabFamEventualCompleto1S,
ifnull(titulares.TrabFamEventualCompleto1Q, 0) TrabFamEventualCompleto1Q,
ifnull(titulares.TrabFamEventualCompleto1M, 0) TrabFamEventualCompleto1M,
ifnull(titulares.TrabFamEventualCompleto2M, 0) TrabFamEventualCompleto2M,
ifnull(titulares.TrabFamEventualCompleto3M, 0) TrabFamEventualCompleto3M,
ifnull(titulares.TrabFamEventualCompleto4M, 0) TrabFamEventualCompleto4M,
ifnull(titulares.TrabFamEventualCompleto5M, 0) TrabFamEventualCompleto5M,
ifnull(titulares.TrabFamEventualCompleto6M, 0) TrabFamEventualCompleto6M,
ifnull(titulares.SBIAguaAdentro, 0) SBIAguaAdentro,
titulares.ServComercializacionAgropecuario,
titulares.ServTransporteAgropecuario,
titulares.PRodRecoleccionMielCanal,
titulares.PRodRecoleccionHongosCanal,
titulares.PRodRecoleccionOtroCanal,
titulares.PAPescaCriaderoTipo,
titulares.PAPescaCriaderoLugar,
titulares.PAPescaRecoleccionTipo,
titulares.PAPescaRecoleccionLugar,
titulares.PAPescaCapturaTipo,
titulares.PAPescaCapturaLugar,
ifnull(titulares.SBIperforacion, 0) SBIperforacion,
ifnull(titulares.SBIpozo, 0) SBIpozo,
ifnull(titulares.SBIlluvia, 0) SBIlluvia,
ifnull(titulares.SBIcisterna, 0) SBIcisterna,
ifnull(titulares.SBIrio, 0) SBIrio,
ifnull(titulares.SBIcanal, 0) SBIcanal,
ifnull(titulares.SBIarroyo, 0) SBIarroyo,
ifnull(titulares.SBIotro, 0) SBIotro,
titulares.Telefono,
titulares.Mail,
titulares.ForzadoRecoleccion,
titulares.ForzadoCaza,
titulares.ForzadoTurismoRural,
ifnull(titulares.AsigPorHijo, 0) AsigPorHijo,
ifnull(titulares.pensionJubilacion, 0) pensionJubilacion,
ifnull(titulares.pensionNoContrib, 0) pensionNoContrib,
ifnull(titulares.pensionGraciable, 0) pensionGraciable,
ifnull(titulares.planAsistencialEmpleo, 0) planAsistencialEmpleo,
ifnull(titulares.seguroDesempleo, 0) seguroDesempleo,
ifnull(titulares.ingresoOtro, 0) ingresoOtro,
ifnull(tipodoc1.id, 1) tdtit_id,
ifnull(tipodoc1.codigo, 0) tdtit_codigo,
tipodoc1.abrev tdtit_abrev,
tipodoc1.descripcion tdtit_descripcion,
ifnull(tipodoc2.id, 1) tdcy_id,
ifnull(tipodoc2.codigo, 0) tdcy_codigo,
tipodoc2.abrev tdcy_abrev,
tipodoc2.descripcion tdcy_descripcion,
ifnull(parentesco1.id, 0) parentesco1_id,
ifnull(parentesco2.id, 0) parentesco2_id,
ifnull(niveled1.id, 0) niveled1_id,
ifnull(niveled2.id, 0) niveled2_id,
tipopiso.Descripcion piso_desc,
tipoparedes.Descripcion paredes_desc,
tipotecho.Descripcion techo_desc
FROM ".SOURCEDB.".titulares titulares
join ".SOURCEDB.".tierra h on titulares.Ventanillaregistro = h.Ventanillaregistro and titulares.pc = h.pc and titulares.Correlativo = h.Correlativo
left join ".TARGETDB.".tipo_documento tipodoc1
on ifnull(titulares.TipoDocumento, '-1') = tipodoc1.codigo
left join ".TARGETDB.".tipo_documento tipodoc2
on ifnull(titulares.CyTipoDocumento, '-1') = tipodoc2.codigo
left join ".TARGETDB.".parentesco parentesco1
on ifnull(titulares.parentesco, '0') = parentesco1.codigo
left join ".TARGETDB.".parentesco parentesco2
on ifnull(titulares.Cyparentesco, '0') = parentesco2.codigo
left join ".TARGETDB.".nivel_educativo niveled1
on ifnull(titulares.NivelEducativo, '0') = niveled1.codigo
left join ".TARGETDB.".nivel_educativo niveled2
on ifnull(titulares.CyNivelEducativo, '0') = niveled2.codigo
left join ".SOURCEDB.".tipopiso tipopiso
on ifnull(titulares.SBIMaterialPisos, '0') = tipopiso.Codigo
left join ".SOURCEDB.".tipoparedes tipoparedes
on ifnull(titulares.SBIMaterialParedes, '0') = tipoparedes.Codigo
left join ".SOURCEDB.".tipotecho tipotecho
on ifnull(titulares.SBIMaterialTecho, '0') = tipotecho.Codigo

WHERE concat('[',titulares.Ventanillaregistro,'][',titulares.pc,'][',titulares.Correlativo,']') not in (select clave from ".SOURCEDB.".log_proceso)
";
 
}

function select_tierra($k1, $k2, $k3) {
	return "select 
Ventanillaregistro Ventanillaregistro,
pc pc,
Correlativo Correlativo,
Domicilio Domicilio,
Paraje Paraje,
ifnull(Municipio, 0) Municipio,
Localidad Localidad,
CP CP,
ifnull(Departamento, 0) Departamento,
ifnull(Provincia, 0) Provincia,
OtraReferencia OtraReferencia,
ifnull(DistanciaAlPredio, 0) DistanciaAlPredio,
DistanciaAlPredioUnidad DistanciaAlPredioUnidad,
EstabDomicilio EstabDomicilio,
EstabParaje EstabParaje,
EstabLocalidad EstabLocalidad,
EstabCP EstabCP,
ifnull(EstabMunicipio, 0) EstabMunicipio,
ifnull(EstabDepartamento, 0) EstabDepartamento,
ifnull(EstabProvincia, 0) EstabProvincia,
EstabOtraReferencia EstabOtraReferencia,
EstabDatoCatastral EstabDatoCatastral,
ifnull(hsPropietario, 0) hsPropietario,
ifnull(hsPropietarioUn, 0) hsPropietarioUn,
ifnull(hsArrendatario, 0) hsArrendatario,
ifnull(hsArrendatarioUn, 0) hsArrendatarioUn,
ifnull(hsMedieria, 0) hsMedieria,
ifnull(hsMedieriaUn, 0) hsMedieriaUn,
ifnull(hsAparceria, 0) hsAparceria,
ifnull(hsAparceriaUn, 0) hsAparceriaUn,
ifnull(hsCondominio, 0) hsCondominio,
ifnull(hsCondominioUn, 0) hsCondominioUn,
ifnull(hsContrato, 0) hsContrato,
ifnull(hsContratoUn, 0) hsContratoUn,
ifnull(hsTFiscales, 0) hsTFiscales,
ifnull(hsTFiscalesUn, 0) hsTFiscalesUn,
ifnull(hsPPrivadas, 0) hsPPrivadas,
ifnull(hsPPrivadasUn, 0) hsPPrivadasUn,
ifnull(hsTPrivadas, 0) hsTPrivadas,
ifnull(hsTPrivadasUn, 0) hsTPrivadasUn,
ifnull(hsIntegrante, 0) hsIntegrante,
ifnull(hsIntegranteUn, 0) hsIntegranteUn,
ifnull(hsPosesionComunitariaIndigena, 0) hsPosesionComunitariaIndigena,
ifnull(hsPosesionComunitariaIndigenaUn, 0) hsPosesionComunitariaIndigenaUn,
ifnull(hsPropiedadComunitariaIndigena, 0) hsPropiedadComunitariaIndigena,
ifnull(hsPropiedadComunitariaIndigenaUn, 0) hsPropiedadComunitariaIndigenaUn,
ifnull(hsOtro, 0) hsOtro,
ifnull(hsOtroUn, 0) hsOtroUn,
ifnull(hsTotal, 0) hsTotal,
ifnull(hsTotalUn, 0) hsTotalUn,
ifnull(supRealTrabajada, 0) supRealTrabajada,
ifnull(supRealTrabajadaUn, 0) supRealTrabajadaUn,
ifnull(ExplotacionFamiliar, 0) ExplotacionFamiliar,
ifnull(ExplotacionAsociativa, 0) ExplotacionAsociativa,
AsocAsociacionCivil AsocAsociacionCivil,
AsocCooperativa AsocCooperativa,
AsocCooperadora AsocCooperadora,
AsocConsorcio AsocConsorcio,
AsocComunidad AsocComunidad,
AsocHuerta AsocHuerta,
AsocGrupo AsocGrupo,
AsocSociedad AsocSociedad,
ESDPosesionComunero ESDPosesionComunero,
ESDPosesionParque ESDPosesionParque,
ESDPosesionTierraFiscal ESDPosesionTierraFiscal,
ESDPosesionOtros ESDPosesionOtros,
ESDTenenciaComunero ESDTenenciaComunero,
ESDTenenciaParque ESDTenenciaParque,
ESDTenenciaTierraFiscal ESDTenenciaTierraFiscal,
ESDTenenciaOtros ESDTenenciaOtros,
ESDArrendamientoComunero ESDArrendamientoComunero,
ESDArrendamientoParque ESDArrendamientoParque,
ESDArrendamientoTierraFiscal ESDArrendamientoTierraFiscal,
ESDArrendamientoOtros ESDArrendamientoOtros,
ESDAparceriaComunero ESDAparceriaComunero,
ESDAparceriaParque ESDAparceriaParque,
ESDAparceriaTierraFiscal ESDAparceriaTierraFiscal,
ESDAparceriaOtros ESDAparceriaOtros,
ESDDerechosoComunero ESDDerechosoComunero,
ESDDerechosoParque ESDDerechosoParque,
ESDDerechosoTierraFiscal ESDDerechosoTierraFiscal,
ESDDerechosoOtros ESDDerechosoOtros,
ESDIntegranteComunero ESDIntegranteComunero,
ESDIntegranteParque ESDIntegranteParque,
ESDIntegranteTierraFiscal ESDIntegranteTierraFiscal,
ESDIntegranteOtros ESDIntegranteOtros,
ESDPosesionComunitariaIndigenaComunero ESDPosesionComunitariaIndigenaComunero,
ESDPosesionComunitariaIndigenaParque ESDPosesionComunitariaIndigenaParque,
ESDPosesionComunitariaIndigenaTierraFiscal ESDPosesionComunitariaIndigenaTierraFiscal,
ESDPosesionComunitariaIndigenaOtros ESDPosesionComunitariaIndigenaOtros,
ifnull(Compartida, 0) Compartida,
ifnull(ComparteCon, 0) ComparteCon,
AsocCivilNombre AsocCivilNombre,
CooperativaNombre CooperativaNombre,
CooperadoraNombre CooperadoraNombre,
ConsorcioNombre ConsorcioNombre,
ComunidadNombre ComunidadNombre,
HuertaNombre HuertaNombre,
GrupoNombre GrupoNombre,
SociedadNombre SociedadNombre,
ifnull(LimitesDefinidos, 0) LimitesDefinidos,
OtrasTexto OtrasTexto,
ifnull(RiesgosGranizo, 0) RiesgosGranizo,
ifnull(RiesgosHelada, 0) RiesgosHelada,
ifnull(RiesgosInundacion, 0) RiesgosInundacion,
ifnull(RiesgosVientos, 0) RiesgosVientos,
ifnull(RiesgosExceso, 0) RiesgosExceso,
ifnull(RiesgosDeficit, 0) RiesgosDeficit,
ifnull(RiesgosIncendio, 0) RiesgosIncendio,
ifnull(RiesgosBiologicos, 0) RiesgosBiologicos,
ifnull(RiesgosComerciales, 0) RiesgosComerciales,
ifnull(RiesgosFinancieros, 0) RiesgosFinancieros,
ifnull(PrevencionMalla, 0) PrevencionMalla,
ifnull(PrevencionQuemadores, 0) PrevencionQuemadores,
ifnull(PrevencionObras, 0) PrevencionObras,
ifnull(PrevencionRiego, 0) PrevencionRiego,
ifnull(PrevencionCobertura, 0) PrevencionCobertura,
ifnull(PrevencionOtra, 0) PrevencionOtra,
ifnull(PrevencionOtraTexto, '') PrevencionOtraTexto,
ifnull(EMB1Cantidad, 0) EMB1Cantidad,
ifnull(EMB1Tipo, '') EMB1Tipo,
ifnull(EMB1Material, '') EMB1Material,
ifnull(EMB1Motor, 'x') EMB1Motor,
ifnull(EMB1Potencia, 0) EMB1Potencia,
ifnull(EMB1Eslora, 0) EMB1Eslora,
ifnull(EMB1Uso, '') EMB1Uso,
ifnull(EMB1Adquisicion, 'x') EMB1Adquisicion,
ifnull(EMB2Cantidad, 0) EMB2Cantidad,
ifnull(EMB2Tipo, '') EMB2Tipo,
ifnull(EMB2Material, '') EMB2Material,
ifnull(EMB2Motor, 'x') EMB2Motor,
ifnull(EMB2Potencia, 0) EMB2Potencia,
ifnull(EMB2Eslora, 0) EMB2Eslora,
ifnull(EMB2Uso, '') EMB2Uso,
ifnull(EMB2Adquisicion, 'x') EMB2Adquisicion
 from tierra 
where Ventanillaregistro = ".$k1." and pc = ".$k2." and Correlativo = ".$k3;
}

function select_prodvegetal($k1, $k2, $k3) {
	return "SELECT
ifnull(p.PVACampo, 0) PVACampo,
ifnull(p.PVCubierta, 0) PVCubierta,
p.PVVolumen PVVolumen,
ifnull(p.PVUn, 0) PVUn,
ifnull(p.PVUnidad, 0) PVUnidad,
ifnull(p.PVAutoconsumo, 0) PVAutoconsumo,
ifnull(p.PVMercado, 0) PVMercado,
ifnull(p.PVIntercambio, 0) PVIntercambio,
ifnull(p.PVPrecioUnitario, 0) PVPrecioUnitario,
	
ifnull(a.Codigo, ifnull(p.pvCodigo, 0)) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(s.Codigo, ifnull(p.PVun, 0)) sup_cod,
ifnull(s.Descripcion, '') suc_desc,

ifnull(e.Codigo, ifnull(p.PVExplotacion, 0)) exp_cod,
ifnull(e.Descripcion, '') exp_desc,

ifnull(v.Codigo, ifnull(p.PVUnidad, 0)) vol_cod,
ifnull(v.Descripcion, '') vol_desc,

ifnull(c.Codigo, ifnull(p.PVCanal, 0)) can_cod,
ifnull(c.Descripcion, '') can_desc

from prodvegetal p
left join actividadvegetal a on p.pvCodigo = a.Codigo
left join tiposuperficie s on p.PVun = s.Codigo
left join tipoexplotacion e on p.PVExplotacion = e.Codigo
left join tipovolumen v on p.PVUnidad = v.Codigo
left join tipocanal c on p.PVCanal = c.Codigo
where p.VentanillaRegistro = ".$k1." and p.pc = ".$k2." and p.CorrelativoTitular = ".$k3 
;
 
}


function select_prodanimal($k1, $k2, $k3) {
	return "select
p.PACodigo PACodigo,
ifnull(p.PASubCodigo, 0) PASubCodigo,
ifnull(p.PAVientres, 0) PAVientres,
ifnull(p.PACabezas, 0) PACabezas,
ifnull(p.PAAutoConsumo, 0) PAAutoConsumo,
ifnull(p.PAMercado, 0) PAMercado,
ifnull(p.PAIntercambio, 0) PAIntercambio,
ifnull(p.PAVolumen, 0) PAVolumen,
ifnull(p.PAUnidad, 0) PAUnidad,
ifnull(p.PAPrecioUnitario, 0) PAPrecioUnitario,
ifnull(p.PAExplotacion, '') PAExplotacion,
p.PACanal PACanal,

ifnull(a.Codigo, ifnull(p.PACodigo, 0)) actani_cod,
ifnull(an.Descripcion, '') actani_desc,

ifnull(a.Codigo, ifnull(p.PASubCodigo, '')) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(e.Codigo, ifnull(p.PAExplotacion, 0)) exp_cod,
ifnull(e.Descripcion, '') exp_desc,

ifnull(v.Codigo, ifnull(p.PAUnidad, 0)) vol_cod,
ifnull(v.Descripcion, '') vol_desc,

ifnull(c.Codigo, ifnull(p.PACanal, 0)) can_cod,
ifnull(c.Descripcion, '') can_desc

from prodanimal p
left join actividadanimal an on p.PACodigo = an.Codigo
left join actividades a on p.PASubCodigo = a.Codigo
left join tipoexplotacion e on p.PAExplotacion = e.Codigo
left join tipovolumen v on p.PAUnidad = v.Codigo
left join tipocanal c on p.PACanal = c.Codigo
where p.VentanillaRegistro = ".$k1." and p.pc = ".$k2." and p.CorrelativoTitular = ".$k3 
;
 
}
		

function select_artesanias($k1, $k2, $k3) {
	return "SELECT
p.ArtesaniaCodigo ArtesaniaCodigo,
p.ArtesaniaSubCodigo ArtesaniaSubCodigo,
p.ArtesaniaExplotacion ArtesaniaExplotacion,
ifnull(p.ArtesaniaCosechaMP, 0) ArtesaniaCosechaMP,
p.ArtesaniaVolumen ArtesaniaVolumen,
ifnull(p.ArtesaniaUnidad, 0) ArtesaniaUnidad,
ifnull(p.ArtesaniaPrecio, 0) ArtesaniaPrecio,
p.ArtesaniaCanal ArtesaniaCanal,

ifnull(a.Codigo, ifnull(p.ArtesaniaCodigo, 0)) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(v.Codigo, ifnull(p.ArtesaniaUnidad, 0)) vol_cod,
ifnull(v.Descripcion, '') vol_desc,

ifnull(c.Codigo, ifnull(p.ArtesaniaCanal, 0)) can_cod,
ifnull(c.Descripcion, '') can_desc

from artesanias p
left join tipoartesania a on p.ArtesaniaCodigo = a.Codigo
left join tipovolumen v on p.ArtesaniaUnidad = v.Codigo
left join tipocanal c on p.ArtesaniaCanal = c.Codigo
where p.VentanillaRegistro = ".$k1." and p.pc = ".$k2." and p.CorrelativoTitular = ".$k3 
;
 
}

function select_agroindustria($k1, $k2, $k3) {
	return "SELECT
p.AgroindustriaCodigo AgroindustriaCodigo,
p.AgroindustriaExplotacion AgroindustriaExplotacion,
ifnull(p.AgroindustriaProduceMP, 0) AgroindustriaProduceMP,
p.AgroindustriaVolumen AgroindustriaVolumen,
ifnull(p.AgroindustriaAutoconsumo, 0) AgroindustriaAutoconsumo,
ifnull(p.AgroindustriaMercado, 0) AgroindustriaMercado,
ifnull(p.AgroindustriaIntercambio, 0) AgroindustriaIntercambio,
ifnull(p.AgroindustriaPrecio, 0) AgroindustriaPrecio,
ifnull(p.AgroindustriaUnidad, 0) AgroindustriaUnidad,
p.AgroindustriaCanal AgroindustriaCanal,

ifnull(a.Codigo, ifnull(p.AgroindustriaCodigo, '')) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(v.Codigo, ifnull(p.AgroindustriaUnidad, 0)) vol_cod,
ifnull(v.Descripcion, '') col_desc,

ifnull(c.Codigo, ifnull(p.AgroindustriaCanal, 0)) can_cod,
ifnull(c.Descripcion, '') can_desc

from agroindustria p
left join tipoagroindustria a on p.AgroindustriaCodigo = a.Codigo
left join tipovolumen v on p.AgroindustriaUnidad = v.Codigo
left join tipocanal c on p.AgroindustriaCanal = c.Codigo
where p.VentanillaRegistro = ".$k1." and p.pc = ".$k2." and p.CorrelativoTitular = ".$k3 
;
 
}

function select_infraestructura($k1, $k2, $k3) {
	return "SELECT
ifnull(p.InfraestructuraCodigo, '') InfraestructuraCodigo,
ifnull(p.InfraestructuraCantidad, 0) InfraestructuraCantidad,
ifnull(p.InfraestructuraAdquisicion, '0') InfraestructuraAdquisicion,

ifnull(a.Codigo, ifnull(p.InfraestructuraCodigo, '')) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(v.Codigo, ifnull(p.InfraestructuraAdquisicion, '')) adq_cod,
ifnull(v.Descripcion, '') adq_desc

from infraestructura p
left join tipoinfraestructura a on p.InfraestructuraCodigo = a.Codigo
left join tipoadquisicion v on p.InfraestructuraAdquisicion = v.Codigo
where p.VentanillaRegistro = ".$k1." and p.pc = ".$k2." and p.CorrelativoTitular = ".$k3 
;
 
}

?>
