package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos


object Proc_PlanPruebas_AutoCastOff {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_CargaMaster",args,com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
    
    val Ano = huemulLib.arguments.getValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.getValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")

    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      var IdTestPlan: String = null
      Control.newStep("Define dataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.raiseError(s"error al intentar abrir archivo de datos: ${DF_RAW.error.controlErrorMessage}")
      }
      Control.newStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicos(huemulLib, Control)    
      TablaMaster.setAutoCast(false)
      TablaMaster.dfFromDf(DF_RAW.dataFrameHuemul.dataFrame, "DF_RAW", "DF_Original")
      
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      val a = huemulLib.spark.catalog.listTables(TablaMaster.getCurrentDataBase).collect()
      if (a.exists { x => x.name.toUpperCase() == TablaMaster.tableName.toUpperCase() }) {
        huemulLib.spark.sql(s"drop table if exists ${TablaMaster.getTable} ")
      } 
      
      val FullPath = new org.apache.hadoop.fs.Path(s"${TablaMaster.getFullNameWithPath}")
      val fs = FullPath.getFileSystem(huemulLib.spark.sparkContext.hadoopConfiguration)
      if (fs.exists(FullPath))
        fs.delete(FullPath, true)
        
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
        
      TablaMaster.TipoValor.setMapping("TipoValor",replaceValueOnUpdate = true,"coalesce(new.TipoValor,'nulo')","coalesce(new.TipoValor,'nulo')")
      TablaMaster.IntValue.setMapping("IntValue")
      TablaMaster.BigIntValue.setMapping("BigIntValue")
      TablaMaster.SmallIntValue.setMapping("SmallIntValue")
      TablaMaster.TinyIntValue.setMapping("TinyIntValue")
      TablaMaster.DecimalValue.setMapping("DecimalValue")
      TablaMaster.RealValue.setMapping("RealValue")
      TablaMaster.FloatValue.setMapping("FloatValue")
      TablaMaster.StringValue.setMapping("StringValue")
      TablaMaster.charValue.setMapping("charValue")
      TablaMaster.timeStampValue.setMapping("timeStampValue")
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.newStep("Ejecución")
      if (!TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)) {
        
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error Esperado", "error por tipos de datos", "con error", s"con error", p_testPlan_IsOK = true)
        Control.RegisterTestPlanFeature("AutoCast Apagado", IdTestPlan)
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error Esperado", "error por tipos de datos", "con error", s"sin error", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("AutoCast Apagado", IdTestPlan)
      }
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error Esperado", "error por tipos de datos", "error = 1013", s"error = ${TablaMaster.errorCode}", TablaMaster.errorCode == 1013)
      Control.RegisterTestPlanFeature("AutoCast Apagado", IdTestPlan)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      
      
      Control.finishProcessOk
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("AutoCast Apagado", IdTestPlan)
        Control.controlError.setError(e, this.getClass.getSimpleName, 1)
        Control.finishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}