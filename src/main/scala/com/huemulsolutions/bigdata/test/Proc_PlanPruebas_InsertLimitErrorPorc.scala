package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata.tables.master._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTableConnector
import com.huemulsolutions.bigdata.tables.HuemulTypeInternalTableType

object Proc_PlanPruebas_InsertLimitErrorPorc {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas error en insert por límite de filas",args,com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
    //huemulLib.spark
    val Ano = huemulLib.arguments.getValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.getValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    val TipoTablaParam: String = huemulLib.arguments.getValue("TipoTabla", null, "Debe especificar TipoTabla (ORC,PARQUET,HBASE,DELTA)")
    var TipoTabla: HuemulTypeStorageType = null
    if (TipoTablaParam == "orc")
        TipoTabla = HuemulTypeStorageType.ORC
    else if (TipoTablaParam == "parquet")
        TipoTabla = HuemulTypeStorageType.PARQUET
    else if (TipoTablaParam == "delta")
        TipoTabla = HuemulTypeStorageType.DELTA
    else if (TipoTablaParam == "hbase")
        TipoTabla = HuemulTypeStorageType.HBASE
    else if (TipoTablaParam == "avro")
        TipoTabla = HuemulTypeStorageType.AVRO
    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      var IdTestPlan: String = null
      
      Control.newStep("Define dataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.raiseError(s"error al intentar abrir archivo de datos: ${DF_RAW.error.controlErrorMessage}")
      }
      
      Control.newStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosNuevosPerc(huemulLib, Control,TipoTabla)      
      TablaMaster.dfFromDf(DF_RAW.dataFrameHuemul.dataFrame,"DF_RAW", "DF_Original")
      
      //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      val a = huemulLib.spark.catalog.listTables(TablaMaster.getCurrentDataBase).collect()
      if (a.exists { x => x.name.toUpperCase() == TablaMaster.tableName.toUpperCase() }) {
        huemulLib.spark.sql(s"drop table if exists ${TablaMaster.getTable} ")
      } 
      
      val FullPath = new org.apache.hadoop.fs.Path(s"${TablaMaster.getFullNameWithPath}")
      val fs = FullPath.getFileSystem(huemulLib.spark.sparkContext.hadoopConfiguration)
      if (fs.exists(FullPath))
        fs.delete(FullPath, true)
        
      if (TipoTablaParam == "hbase") {
        Control.newStep("borrar tabla")
        val th = new HuemulTableConnector(huemulLib, Control)
        th.tableDeleteHBase(TablaMaster.getHBaseNamespace(HuemulTypeInternalTableType.Normal), TablaMaster.getHBaseTableName(HuemulTypeInternalTableType.Normal))
      }
        
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
      
      Control.newStep("PASO 1: INSERTA NORMAL")
      if (!TablaMaster.executeFull("DF_Final_Todo", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)     
        Control.raiseError(s"error al masterizar (${TablaMaster.errorCode}): ${TablaMaster.errorText}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", p_testPlan_IsOK = true)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)
      }
      TablaMaster.dataFrameHuemul.dataFrame.show()
       Control.newStep("Define dataFrame Original")
      
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"Nuevos")) {
        Control.raiseError(s"error al intentar abrir archivo de datos: ${DF_RAW.error.controlErrorMessage}")
      }
      TablaMaster.dfFromDf(DF_RAW.dataFrameHuemul.dataFrame,"DF_RAW", "DF_Nuevos")
      
      val DFValidaCantIni = huemulLib.dfExecuteQuery("validaCantidad", s"select cast(count(1) as Long) as Cantidad from ${TablaMaster.getTable}")
      val NumReg = DFValidaCantIni.first().getAs[Long]("Cantidad")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "NumRegInicial", "N° Registros iniciales de tablas", "N° Reg Inicial = 6", s"N° Reg Inicial = $NumReg", NumReg == 6)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)
        
      Control.newStep("PASO 1: INSERTA NUEVOS")
      if (!TablaMaster.executeOnlyInsert("DF_Final_Todo", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "Si hay error en masterización", "Si hay error en masterización", s"Si hay error en masterización", p_testPlan_IsOK = true)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)     
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)
      }
      TablaMaster.dataFrameHuemul.dataFrame.show()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "Si hay error en masterización", "Si hay error en masterización 1006", s"Si hay error en masterización (${TablaMaster.errorCode})", TablaMaster.errorCode == 1006)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)
      
      val DFValidaCantFin = huemulLib.dfExecuteQuery("validaCantidad", s"select cast(count(1) as Long) as Cantidad from ${TablaMaster.getTable}")
      val NumRegFin = DFValidaCantFin.first().getAs[Long]("Cantidad")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "NumRegFinal", "N° Registros Finales de tablas", "N° Reg Finales = 6", s"N° Reg Finales = $NumRegFin", NumRegFin == 6)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)
      
          Control.finishProcessOk
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Perc", IdTestPlan)
        Control.controlError.setError(e, this.getClass.getSimpleName, 1)
        Control.finishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}