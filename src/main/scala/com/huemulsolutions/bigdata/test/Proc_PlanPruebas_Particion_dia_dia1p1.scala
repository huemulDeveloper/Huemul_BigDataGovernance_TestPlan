package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.master.tbl_DatosParticion

/**
 * Este plan de pruebas valida lo siguiente:
 * error en PK: hay registros duplicados, lo que se espera es un error de PK
 * el TipodeArchivo usado es Malo01
 */
object Proc_PlanPruebas_Particion_dia_dia1p1 {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Proc_PlanPruebas_Particion_dia",args,com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
    var empresaName: String = "EmpresA"

    //val empresa = huemulLib.arguments.getValue("empresa", null,"Debe especificar una empresa, ejemplo: empresa=super-01")
    val TipoTablaParam: String = huemulLib.arguments.getValue("TipoTabla", null, "Debe especificar TipoTabla (ORC,PARQUET,HBASE,DELTA)")
    var TipoTabla: HuemulTypeStorageType = null
    if (TipoTablaParam == "orc")
      TipoTabla = HuemulTypeStorageType.ORC
    else if (TipoTablaParam == "parquet")
      TipoTabla = HuemulTypeStorageType.PARQUET
    else if (TipoTablaParam == "delta")
      TipoTabla = HuemulTypeStorageType.DELTA
    else if (TipoTablaParam == "hbase")
      TipoTabla = HuemulTypeStorageType.PARQUET
    else if (TipoTablaParam == "avro") {
      TipoTabla = HuemulTypeStorageType.AVRO
      empresaName = empresaName.toLowerCase()
    }

    val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    var IdTestPlan: String = ""
    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.newStep("Ejecuta pruebas con dia 01")
      val args_01: Array[String] = new Array[String](1)
      val Ano = 2017
      val Mes = 5
      val dia = 1
      val empresa = "super-01"
      args_01(0) = s"Environment=production,ano=$Ano,mes=$Mes,dia=$dia,empresa=$empresa,TipoTabla=$TipoTablaParam"
      val control_resultado01 = Proc_PlanPruebas_Particion_dia.processMaster(huemulLib, args_01)

      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Resultado Ejecucion dia 01", "Resultado Ejecución 01", "error = false", s"error = ${control_resultado01.controlError.controlErrorIsError}", !control_resultado01.controlError.controlErrorIsError)

      //abre instancia de tabla para obtener algunos parámetros
      val TablaMaster = new tbl_DatosParticion(huemulLib, Control, TipoTabla)

      //valida que existan las particiones esperadas
      val path_20170501_super01_internet = TablaMaster.getFullNameWithPath.concat(s"/periodo=2017-05-01/$empresaName=super-01/app=internet")
      val path_20170501_super01_tienda = TablaMaster.getFullNameWithPath.concat(s"/periodo=2017-05-01/$empresaName=super-01/app=tienda")

      Control.newStep(s"buscando path $path_20170501_super01_internet")
      var path_existe = huemulLib.hdfsPath_exists(path_20170501_super01_internet)
      IdTestPlan = Control.RegisterTestPlan(
           TestPlanGroup
        , "dia 01 - existe path path_20170501_super01_internet"
        , s"path buscado = $path_20170501_super01_internet"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)

      Control.newStep(s"buscando path $path_20170501_super01_tienda")
      path_existe = huemulLib.hdfsPath_exists(path_20170501_super01_tienda)
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 01 - existe path path_20170501_super01_tienda"
        , s"path buscado = $path_20170501_super01_tienda"
        , "path existe = true"
        , s"path existe = $path_existe"
        , path_existe)


      huemulLib.spark.sql("select * from production_master.tbl_datosparticion").show()



      val sql_01 = s"""
           SELECT periodo, empresa, app, cast(count(1) as Integer) as cantidad
           FROM production_master.tbl_datosparticion
           GROUP BY periodo, empresa, app
           """
      println(sql_01)

      //valida que datos particionados existan
      val DF_valida01 = huemulLib.spark.sql(
        sql_01)

      DF_valida01.show()

      val count_01 = DF_valida01.count()
      //muestra datos ejemplos
      IdTestPlan = Control.RegisterTestPlan(
        TestPlanGroup
        , "dia 01 - calida cantidad de datos totales"
        , s"debe arrojar 2 registros"
        , "registros = 2"
        , s"registros = $count_01"
        , count_01 == 2)

      val registro01_01 = DF_valida01.where("""periodo = "2017-05-01" and empresa = "super-01" and app = "internet"""").select("cantidad").first()
      if (registro01_01 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | internet"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_01.getAs[Integer]("cantidad")}"
          , registro01_01.getAs[Integer]("cantidad") == 2)
      }

      val registro01_02 = DF_valida01.where("periodo = '2017-05-01' and empresa = 'super-01' and app = 'tienda'").select("cantidad").first()
      if (registro01_02 == null) {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | tieda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros no encontrados"
          , p_testPlan_IsOK = false)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(
          TestPlanGroup
          , "dia 01 - existe registro 2017-05-01 | super-01 | tieda"
          , s"debe arrojar 2 registros"
          , "registros = 2"
          , s"registros = ${registro01_02.getAs[Integer]("cantidad")}"
          , registro01_02.getAs[Integer]("cantidad") == 2)
      }


      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      
      Control.finishProcessOk
    } catch {
      case e: Exception => 
        
        Control.RegisterTestPlan(TestPlanGroup, "error en pruebas", "ERROR DE PROGRAMA -  NO deberia tener errror", "sin errores", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.controlError.setError(e, this.getClass.getSimpleName, 1)
        Control.finishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(6))
      println("Proceso OK")
    
    huemulLib.close()
  }
}