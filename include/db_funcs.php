<?php
define("SOURCEDB", "renaf_amigrar");
define("TARGETDB", "renaf_migrada");

function db_query($db_conn, $query) {
	if (!$result = $db_conn->query($query)) {
		die("Invalid query: " . $db_conn->error."<BR>".$query);	
	}
	return $result;
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
titulares.ServAlqTierra,
titulares.ServSuperficieAlquilada,
titulares.ServSuperficieAlquiladaUnidades,
titulares.ServVtaFuerzaTrab,
titulares.ServVtaFuerzaTrabAgropecuario,
titulares.ServVtaFuerzaTrabEv,
titulares.ServVtaFuerzaTrabEvAgropecuario,
titulares.ServUsoANimales,
titulares.ServUsoANimalesAgropecuario,
titulares.ServUsoMaquinarias,
titulares.ServUsoMaquinariasAgropecuario,
titulares.ServTurismo,
titulares.ServTransporte,
titulares.ServComercializacion,
titulares.ProdVegetalOrganica,
titulares.ProdVegetalOrganicaCertificada,
ifnull(titulares.ProdAnimalOrganica, 0) ProdAnimalOrganica,
ifnull(titulares.ProdAnimalOrganicaCertificada, 0) ProdAnimalOrganicaCertificada,
titulares.ProdAgrindustriaOrganica,
titulares.ProdAgrindustriaOrganicaCertificada,
titulares.PRodCazaCarneAutoConsumo,
titulares.PRodCazaCarneMercado,
titulares.PRodCazaCarneIntercambio,
titulares.PRodCazaCarneVolumen,
ifnull(titulares.PRodCazaCarneUnidad, 0) PRodCazaCarneUnidad,
titulares.PRodCazaCarnePrecio,
titulares.PRodCazaCarneExplotacion,
titulares.PRodCazaCuerosAutoConsumo,
titulares.PRodCazaCuerosMercado,
titulares.PRodCazaCuerosIntercambio,
titulares.PRodCazaCuerosVolumen,
ifnull(titulares.PRodCazaCuerosUnidad, 0) PRodCazaCuerosUnidad,
titulares.PRodCazaCuerosPrecio,
titulares.PRodCazaCuerosExplotacion,
titulares.PRodCazaOtrosAutoConsumo,
titulares.PRodCazaOtrosMercado,
titulares.PRodCazaOtrosIntercambio,
titulares.PRodCazaOtrosVolumen,
ifnull(titulares.PRodCazaOtrosUnidad, 0) PRodCazaOtrosUnidad,
titulares.PRodCazaOtrosPrecio,
titulares.PRodCazaOtrosExplotacion,
titulares.PRodRecoleccionVegetalesAutoConsumo,
titulares.PRodRecoleccionVegetalesMercado,
titulares.PRodRecoleccionVegetalesIntercambio,
titulares.PRodRecoleccionVegetalesVolumen,
ifnull(titulares.PRodRecoleccionVegetalesUnidad, 0) PRodRecoleccionVegetalesUnidad,
titulares.PRodRecoleccionVegetalesPrecio,
titulares.PRodRecoleccionVegetalesExplotacion,
titulares.PRodRecoleccionMineralesAutoConsumo,
titulares.PRodRecoleccionMineralesMercado,
titulares.PRodRecoleccionMineralesIntercambio,
titulares.PRodRecoleccionMineralesVolumen,
ifnull(titulares.PRodRecoleccionMineralesUnidad, 0) PRodRecoleccionMineralesUnidad,
titulares.PRodRecoleccionMineralesPrecio,
titulares.PRodRecoleccionMineralesExplotacion,
titulares.PRodRecoleccionVegetalCanal,
titulares.PRodRecoleccionMineralCanal,
titulares.PRodRecoleccionHongosAutoConsumo,
titulares.PRodRecoleccionHongosMercado,
titulares.PRodRecoleccionHongosIntercambio,
titulares.PRodRecoleccionHongosVolumen,
ifnull(titulares.PRodRecoleccionHongosUnidad, 0) PRodRecoleccionHongosUnidad,
titulares.PRodRecoleccionHongosPrecio,
titulares.PRodRecoleccionHongosExplotacion,
titulares.PRodRecoleccionMielAutoConsumo,
titulares.PRodRecoleccionMielMercado,
titulares.PRodRecoleccionMielIntercambio,
titulares.PRodRecoleccionMielVolumen,
ifnull(titulares.PRodRecoleccionMielUnidad, 0) PRodRecoleccionMielUnidad,
titulares.PRodRecoleccionMielPrecio,
titulares.PRodRecoleccionMielExplotacion,
titulares.PRodRecoleccionOtroAutoConsumo,
titulares.PRodRecoleccionOtroMercado,
titulares.PRodRecoleccionOtroIntercambio,
titulares.PRodRecoleccionOtroVolumen,
ifnull(titulares.PRodRecoleccionOtroUnidad, 0) PRodRecoleccionOtroUnidad, 
titulares.PRodRecoleccionOtroPrecio,
titulares.PRodRecoleccionOtroExplotacion,
titulares.PRodRecoleccionOtroTexto,
titulares.PRodCazaCarneCanal,
titulares.PRodCazaCuerosCanal,
titulares.PRodCazaOtrosCanal,
titulares.PRodCazaOtrosTexto,
titulares.Riego,
titulares.RiegoSuperficie,
titulares.RiegoUnidades,
titulares.RiegoTipo,
titulares.AguaConsumoAnimal,
titulares.TraccionAnimal,
titulares.TR1Cantidad,
titulares.TR1Potencia,
titulares.TR1Modelo,
titulares.TR1Adquisicion,
titulares.TR1Propiedad,
titulares.TR2Cantidad,
titulares.TR2Potencia,
titulares.TR2Modelo,
titulares.TR2Adquisicion,
titulares.TR2Propiedad,
titulares.VH1Cantidad,
titulares.VH1Tipo,
titulares.VH1Modelo,
titulares.VH1Adquisicion,
titulares.VH1Propiedad,
titulares.VH2Cantidad,
titulares.VH2Tipo,
titulares.VH2Modelo,
titulares.VH2Adquisicion,
titulares.VH2Propiedad,
ifnull(titulares.PAApiculturaColmenasPropias, 0) PAApiculturaColmenasPropias,
ifnull(titulares.PAApiculturaColmenasTerceros, 0) PAApiculturaColmenasTerceros,
titulares.PAApiculturaMielColmenas,
titulares.PAApiculturaMielExplotacion,
titulares.PAApiculturaMielAutoConsumo,
titulares.PAApiculturaMielMercado,
titulares.PAApiculturaMielIntercambio,
titulares.PAApiculturaMielVolumen,
ifnull(titulares.PAApiculturaMielUnidades, 0) PAApiculturaMielUnidades,
titulares.PAApiculturaMielPrecioUnitario,
titulares.PAApiculturaCeraColmenas,
titulares.PAApiculturaCeraExplotacion,
titulares.PAApiculturaCeraAutoConsumo,
titulares.PAApiculturaCeraMercado,
titulares.PAApiculturaCeraIntercambio,
titulares.PAApiculturaCeraVolumen,
ifnull(titulares.PAApiculturaCeraUnidades, 0) PAApiculturaCeraUnidades,
titulares.PAApiculturaCeraPrecioUnitario,
titulares.PAApiculturaPropoleoColmenas,
titulares.PAApiculturaPropoleoExplotacion,
titulares.PAApiculturaPropoleoAutoConsumo,
titulares.PAApiculturaPropoleoMercado,
titulares.PAApiculturaPropoleoIntercambio,
titulares.PAApiculturaPropoleoVolumen,
ifnull(titulares.PAApiculturaPropoleoUnidades, 0) PAApiculturaPropoleoUnidades,
titulares.PAApiculturaPropoleoPrecioUnitario,
titulares.PAApiculturaJaleaColmenas,
titulares.PAApiculturaJaleaExplotacion,
titulares.PAApiculturaJaleaAutoConsumo,
titulares.PAApiculturaJaleaMercado,
titulares.PAApiculturaJaleaIntercambio,
titulares.PAApiculturaJaleaVolumen,
ifnull(titulares.PAApiculturaJaleaUnidades, 0) PAApiculturaJaleaUnidades,
titulares.PAApiculturaJaleaPrecioUnitario,
titulares.PAApiculturaPolenColmenas,
titulares.PAApiculturaPolenExplotacion,
titulares.PAApiculturaPolenAutoConsumo,
titulares.PAApiculturaPolenMercado,
titulares.PAApiculturaPolenIntercambio,
titulares.PAApiculturaPolenVolumen,
ifnull(titulares.PAApiculturaPolenUnidades, 0) PAApiculturaPolenUnidades,
titulares.PAApiculturaPolenPrecioUnitario,
titulares.PAApiculturaNucleosColmenas,
titulares.PAApiculturaNucleosExplotacion,
titulares.PAApiculturaNucleosAutoConsumo,
titulares.PAApiculturaNucleosMercado,
titulares.PAApiculturaNucleosIntercambio,
titulares.PAApiculturaNucleosVolumen,
ifnull(titulares.PAApiculturaNucleosUnidades, 0) PAApiculturaNucleosUnidades,
titulares.PAApiculturaNucleosPrecioUnitario,
titulares.PAApiculturaMaterialVivoColmenas,
titulares.PAApiculturaMaterialVivoExplotacion,
titulares.PAApiculturaMaterialVivoAutoConsumo,
titulares.PAApiculturaMaterialVivoMercado,
titulares.PAApiculturaMaterialVivoIntercambio,
titulares.PAApiculturaMaterialVivoVolumen,
ifnull(titulares.PAApiculturaMaterialVivoUnidades, 0) PAApiculturaMaterialVivoUnidades, 
titulares.PAApiculturaMaterialVivoPrecioUnitario,
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
titulares.PAApiculturaOtroPrecioUnitario,
titulares.PAPescaCriaderoExplotacion,
titulares.PAPescaCriaderoAutoConsumo,
titulares.PAPescaCriaderoMercado,
titulares.PAPescaCriaderoIntercambio,
titulares.PAPescaCriaderoVolumen,
titulares.PAPescaCriaderoUnidad,
titulares.PAPescaCriaderoPrecio,
titulares.PAPescaRecoleccionExplotacion,
titulares.PAPescaRecoleccionAutoConsumo,
titulares.PAPescaRecoleccionMercado,
titulares.PAPescaRecoleccionIntercambio,
titulares.PAPescaRecoleccionVolumen,
titulares.PAPescaRecoleccionUnidad,
titulares.PAPescaRecoleccionPrecio,
titulares.PAPescaCapturaExplotacion,
titulares.PAPescaCapturaAutoConsumo,
titulares.PAPescaCapturaMercado,
titulares.PAPescaCapturaIntercambio,
titulares.PAPescaCapturaVolumen,
titulares.PAPescaCapturaUnidad,
titulares.PAPescaCapturaPrecio,
titulares.PAPescaCapturaCanal,
titulares.PAPescaCriaderoCanal,
titulares.PAPescaRecoleccionCanal,
titulares.PTRenPredio,
titulares.PTRenPredioExplotacion,
titulares.PTRenPredioIngresos,
titulares.PTRfueraPredio,
titulares.PTRFueraPredioExplotacion,
titulares.PTRfueraPredioIngresos,
titulares.Duenios,
titulares.DecidenVender,
titulares.SemillasProduccioPropia,
titulares.SemillasCompra,
titulares.SemillasSubsudiado,
titulares.AnimalesMejoraIA,
titulares.AnimalesMejoraSeleccion,
titulares.AnimalesMejoraCruzamiento,
titulares.AnimalesMejoraOtra,
titulares.AnimalesMejoraTexto,
titulares.AbonosOrganicosProduccion,
titulares.AbonosOrganicosCompra,
titulares.AbonosOrganicosSubsidiado,
titulares.AbonosQuimicosProduccion,
titulares.AbonosQuimicosCompra,
titulares.AbonosQuimicosSubsidiado,
titulares.ControlPlagaNoQuimicos,
titulares.ControlPlagaBiologicos,
titulares.ControlPlagaOtrosMetodos,
titulares.ControlPlagaQuimicos,
titulares.Rotacion,
titulares.TrabPermanentesCompleta,
titulares.TrabPermanentesParcial,
titulares.Preparacion1S,
titulares.Preparacion1Q,
titulares.Preparacion1M,
titulares.Preparacion2M,
titulares.Preparacion3M,
titulares.Preparacion4M,
titulares.Preparacion5M,
titulares.Preparacion6M,
titulares.Siembra1S,
titulares.Siembra1Q,
titulares.Siembra1M,
titulares.Siembra2M,
titulares.Siembra3M,
titulares.Siembra4M,
titulares.Siembra5M,
titulares.Siembra6M,
titulares.Labores1S,
titulares.Labores1Q,
titulares.Labores1M,
titulares.Labores2M,
titulares.Labores3M,
titulares.Labores4M,
titulares.Labores5M,
titulares.Labores6M,
titulares.Cosecha1S,
titulares.Cosecha1Q,
titulares.Cosecha1M,
titulares.Cosecha2M,
titulares.Cosecha3M,
titulares.Cosecha4M,
titulares.Cosecha5M,
titulares.Cosecha6M,
titulares.Otras1S,
titulares.Otras1Q,
titulares.Otras1M,
titulares.Otras2M,
titulares.Otras3M,
titulares.Otras4M,
titulares.Otras5M,
titulares.Otras6M,
titulares.OtrasTexto,
titulares.Maquinaria,
titulares.ServPreparacionSup,
titulares.ServPreparacionUni,
titulares.ServPreparacionPersonas,
titulares.ServPreparacionDias,
titulares.ServSiembraSup,
titulares.ServSiembreUni,
titulares.ServSiembraPersonas,
titulares.ServSiembraDias,
titulares.ServCulturalesSup,
titulares.ServCulturalesUni,
titulares.ServCulturalesPersonas,
titulares.ServCulturalesDias,
titulares.ServCosechaSup,
titulares.ServCosechaUni,
titulares.ServCosechaPersonas,
titulares.ServCosechaDias,
titulares.ServOtraSup,
titulares.ServOtraUni,
titulares.ServOtraPersonas,
titulares.ServOtraDias,
titulares.ServOtraTexto,
titulares.Asesoramiento,
titulares.AsesoraminetoPaga,
titulares.AsesoramientoPrograma,
titulares.SBICentroProcCercano,
titulares.SBICentroProcUni,
titulares.SBIMercadoCercano,
titulares.SBIMercadoUni,
titulares.SBIMaterialPisos,
titulares.SBIMaterialParedes,
titulares.SBIRevoque,
titulares.SBIMaterialTecho,
titulares.VIVDormitorios,
titulares.VIVBanio,
titulares.VIVCocina,
titulares.SBCloaca,
titulares.SBDesague,
titulares.SBIAguaRed,
titulares.SBIAguaOtro,
titulares.SBIAguaTipo,
titulares.SBILuz,
titulares.SBIGas,
titulares.SBIGasEnvasado,
titulares.SBILenia,
titulares.SBIOtros,
titulares.SBIOtrosTexto,
titulares.SBIRedVial,
titulares.SBICaminos,
titulares.SaludCoberturaOsocial,
titulares.SaludCoberturaPrePaga,
titulares.SaludCoberturaEstatal,
titulares.SaludSinCobertura,
titulares.SALUDHospitalSiNo,
titulares.SALUDHospital,
titulares.SALUDHospitalUni,
titulares.SALUDDispensarioSiNo,
titulares.SALUDDispensario,
titulares.SALUDDispernsarioUni,
titulares.SALUDClinicaOSSiNo,
titulares.SALUDClinicaOS,
titulares.SALUDClinicaOSUni,
titulares.SALUDClinicaPrepSiNo,
titulares.SALUDClinicaPrep,
titulares.SALUDClinicaPrepUni,
titulares.EDUCGuarderia,
titulares.EDUCGuarderiaUni,
titulares.EDUCJardin,
titulares.EDUCJardinUni,
titulares.EDUCPrimario,
titulares.EDUCPrimarioUni,
titulares.EDUCSecundario,
titulares.EDUCSecundarioUni,
titulares.EDUCTerciario,
titulares.EDUCTerciarioUni,
titulares.EDUCEducEspecial,
titulares.EDUCEducEspecialUni,
titulares.IngBrProduccion,
titulares.IngBrServicios,
titulares.IngBrOtros,
titulares.ComunidadOriginarios,
titulares.TrabFamPermanentesParcial,
titulares.TrabFamPermanentesCompleto,
titulares.TrabFamEventualParcial1S,
titulares.TrabFamEventualParcial1Q,
titulares.TrabFamEventualParcial1M,
titulares.TrabFamEventualParcial2M,
titulares.TrabFamEventualParcial3M,
titulares.TrabFamEventualParcial4M,
titulares.TrabFamEventualParcial5M,
titulares.TrabFamEventualParcial6M,
titulares.TrabFamEventualCompleto1S,
titulares.TrabFamEventualCompleto1Q,
titulares.TrabFamEventualCompleto1M,
titulares.TrabFamEventualCompleto2M,
titulares.TrabFamEventualCompleto3M,
titulares.TrabFamEventualCompleto4M,
titulares.TrabFamEventualCompleto5M,
titulares.TrabFamEventualCompleto6M,
titulares.SBIAguaAdentro,
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
titulares.SBIperforacion,
titulares.SBIpozo,
titulares.SBIlluvia,
titulares.SBIcisterna,
titulares.SBIrio,
titulares.SBIcanal,
titulares.SBIarroyo,
titulares.SBIotro,
titulares.Telefono,
titulares.Mail,
titulares.ForzadoRecoleccion,
titulares.ForzadoCaza,
titulares.ForzadoTurismoRural,
titulares.AsigPorHijo,
titulares.pensionJubilacion,
titulares.pensionNoContrib,
titulares.pensionGraciable,
titulares.planAsistencialEmpleo,
titulares.seguroDesempleo,
titulares.ingresoOtro,
tipodoc1.id tdtit_id,
tipodoc1.codigo tdtit_codigo,
tipodoc1.abrev tdtit_abrev,
tipodoc1.descripcion tdtit_descripcion,
tipodoc2.id tdcy_id,
tipodoc2.codigo tdcy_codigo,
tipodoc2.abrev tdcy_abrev,
tipodoc2.descripcion tdcy_descripcion,
ifnull(parentesco1.id, 0) parentesco1_id,
ifnull(parentesco2.id, 0) parentesco2_id,
ifnull(niveled1.id, 0) niveled1_id,
ifnull(niveled2.id, 0) niveled2_id
FROM titulares titulares
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
";
 
}

function select_prodvegetal($k1, $k2, $k3) {
	return "SELECT
p.PVACampo PVACampo,
p.PVCubierta PVCubierta,
p.PVVolumen PVVolumen,
p.PVUn PVUn,
ifnull(p.PVUnidad, 0) PVUnidad,
p.PVAutoconsumo PVAutoconsumo,
p.PVMercado PVMercado,
p.PVIntercambio PVIntercambio,
p.PVPrecioUnitario PVPrecioUnitario,
	
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
p.PAAutoConsumo PAAutoConsumo,
p.PAMercado PAMercado,
p.PAIntercambio PAIntercambio,
p.PAVolumen PAVolumen,
ifnull(p.PAUnidad, 0) PAUnidad,
p.PAPrecioUnitario PAPrecioUnitario,
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
p.ArtesaniaUnidad ArtesaniaUnidad,
p.ArtesaniaPrecio ArtesaniaPrecio,
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
p.AgroindustriaProduceMP AgroindustriaProduceMP,
p.AgroindustriaVolumen AgroindustriaVolumen,
p.AgroindustriaAutoconsumo AgroindustriaAutoconsumo,
p.AgroindustriaMercado AgroindustriaMercado,
ifnull(p.AgroindustriaIntercambio, 0) AgroindustriaIntercambio,
p.AgroindustriaPrecio AgroindustriaPrecio,
p.AgroindustriaUnidad AgroindustriaUnidad,
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
p.InfraestructuraCodigo InfraestructuraCodigo,
p.InfraestructuraCantidad InfraestructuraCantidad,
p.InfraestructuraAdquisicion InfraestructuraAdquisicion,

ifnull(a.Codigo, ifnull(p.InfraestructuraCodigo, '')) act_cod,
ifnull(a.Descripcion, '') act_desc,

ifnull(v.Codigo, ifnull(p.InfraestructuraAdquisicion, '')) vol_cod,
ifnull(v.Descripcion, '') col_desc

from infraestructura p
left join tipoinfraestructura a on p.InfraestructuraCodigo = a.Codigo
left join tipoadquisicion v on p.InfraestructuraAdquisicion = v.Codigo
where p.VentanillaRegistro = ".$k1." and p.pc = ".$k2." and p.CorrelativoTitular = ".$k3 
;
 
}

function deletes() {
$delete_naf_completo = "
delete FROM renaf.naf_completo_integrante;
delete FROM renaf.integrante;
delete FROM renaf.planilla_completa_action_alertado;
delete FROM renaf.auditoria_planilla_completa;
delete FROM renaf.transicionpr_planilla_completa;
delete FROM renaf.planilla_completa;
delete FROM renaf.naf_completo;
";

$delete_persona = "
delete FROM renaf.auditoria_planilla_reducida;
delete FROM renaf.planilla_reducida_action_alertado;
delete FROM renaf.transicionfid_planilla_reducida;
delete FROM renaf.planilla_reducida;
delete FROM renaf.titular_completa;
delete FROM renaf.persona;
";
		
	
}
?>
