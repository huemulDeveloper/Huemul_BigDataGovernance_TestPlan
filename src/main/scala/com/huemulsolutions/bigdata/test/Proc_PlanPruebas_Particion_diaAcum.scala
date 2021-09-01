package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosParticion
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.master.tbl_DatosParticionAcum

/**
 * Este plan de pruebas valida lo siguiente:
 * Error en PK: hay registros duplicados, lo que se espera es un error de PK
 * el TipodeArchivo usado es Malo01
 */
object Proc_PlanPruebas_Particion_diaAcum {
  def main(args: Array[String]): Unit = {
    processMaster(null,args)
  }

  def processMaster(huemulLib2: HuemulBigDataGovernance, args: Array[String]): HuemulControl = {
    val huemulLib = if (huemulLib2 == null) new HuemulBigDataGovernance("01 - Proc_PlanPruebas_Particion_dia",args,com.yourcompany.settings.globalSettings.Global) else huemulLib2
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)

    /*
    if (huemulLib.GlobalSettings.getBigDataProvider() == HuemulTypeBigDataProvider.databricks) {
      huemulLib.spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")
    }

     */
    huemulLib.arguments.setArgs(args)
    val Ano = huemulLib.arguments.getValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.getValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    val dia  = huemulLib.arguments.getValue("dia", null,"Debe especificar dia de proceso: ejemplo: dia=1")
    val empresa = huemulLib.arguments.getValue("empresa", null,"Debe especificar una empresa, ejemplo: empresa=super-01")
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
    else if (TipoTablaParam == "avro")
      TipoTabla = HuemulTypeStorageType.AVRO

    //val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    //var IdTestPlan: String = ""
    //Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosParticion(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, dia.toInt, 0, 0, 0,empresa)) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosParticionAcum(huemulLib, Control, TipoTabla)

      TablaMaster.DF_from_SQL("df_data",
        """SELECT to_date(periodo,'yyyyMMdd') as periodo,
              empresa,
              app,
              producto,
              cantidad,
              precio,
              idTx
              FROM DF_RAW
          """)

      TablaMaster.periodo.setMapping("periodo")
      TablaMaster.EmpresA.setMapping("empresa")
      TablaMaster.app.setMapping("app")
      TablaMaster.producto.setMapping("producto")
      TablaMaster.cantidad.setMapping("cantidad")
      TablaMaster.precio.setMapping("precio")
      TablaMaster.idTx.setMapping("idTx")

      if (!TablaMaster.executeFull("DF_FinalParticion")) {
        Control.RaiseError("Error al masterizar")
        println("error al masterizar")
      }

  
      
      //IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyInsert","Pudo agregar acceso", "no Pudo agregar acceso", s"Pudo agregar acceso", false)
      //Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyInsert", IdTestPlan)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => 
        
        ///val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyInsert", "ERROR DE PROGRAMA -  deberia tener errror", "con error 1033", s"con error: ${Control.Control_Error.ControlError_ErrorCode}", Control.Control_Error.ControlError_ErrorCode == 1033)
        //Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyInsert", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, Control.Control_Error.ControlError_ErrorCode)
        Control.FinishProcessError()
    }
    
    //if (Control.TestPlan_CurrentIsOK(null))
    //  println("Proceso OK")
    
    //huemulLib.close()

    Control
  }
}