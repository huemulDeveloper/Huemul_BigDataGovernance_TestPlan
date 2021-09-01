package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata.tables.master._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTableConnector
import com.huemulsolutions.bigdata.tables.HuemulTypeInternalTableType

object Proc_PlanPruebas_InsertLimitError {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas error en insert por límite de filas",args,com.yourcompany.settings.globalSettings.Global)
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
      
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosNuevos(huemulLib, Control,TipoTabla)      
      TablaMaster.DF_from_DF(DF_RAW.DataFramehuemul.DataFrame,"DF_RAW", "DF_Original")
      
      //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      val a = huemulLib.spark.catalog.listTables(TablaMaster.getCurrentDataBase).collect()
      if (a.exists { x => x.name.toUpperCase() == TablaMaster.TableName.toUpperCase() }) {
        huemulLib.spark.sql(s"drop table if exists ${TablaMaster.getTable} ")
      } 
      
      val FullPath = new org.apache.hadoop.fs.Path(s"${TablaMaster.getFullNameWithPath}")
      val fs = FullPath.getFileSystem(huemulLib.spark.sparkContext.hadoopConfiguration)
      if (fs.exists(FullPath))
        fs.delete(FullPath, true)
        
      if (TipoTablaParam == "hbase") {
        Control.NewStep("borrar tabla")
        val th = new HuemulTableConnector(huemulLib, Control)
        th.tableDeleteHBase(TablaMaster.getHBaseNamespace(HuemulTypeInternalTableType.Normal), TablaMaster.getHBaseTableName(HuemulTypeInternalTableType.Normal))
      }
        
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
      
    
      TablaMaster.TipoValor.setMapping("TipoValor",ReplaceValueOnUpdate = true,"coalesce(new.TipoValor,'nulo')","coalesce(new.TipoValor,'nulo')")
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
      
      Control.NewStep("PASO 1: INSERTA NORMAL")
      if (!TablaMaster.executeFull("DF_Final_Todo", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"Si hay error en masterización", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)     
        Control.RaiseError(s"Error al masterizar (${TablaMaster.Error_Code}): ${TablaMaster.Error_Text}")
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", p_testPlan_IsOK = true)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
      }
      TablaMaster.DataFramehuemul.DataFrame.show()
       Control.NewStep("Define DataFrame Original")
      
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"Nuevos")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      TablaMaster.DF_from_DF(DF_RAW.DataFramehuemul.DataFrame,"DF_RAW", "DF_Nuevos")
      
      val DFValidaCantIni = huemulLib.DF_ExecuteQuery("validaCantidad", s"select cast(count(1) as Long) as Cantidad from ${TablaMaster.getTable}")
      val NumReg = DFValidaCantIni.first().getAs[Long]("Cantidad")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "NumRegInicial", "N° Registros iniciales de tablas", "N° Reg Inicial = 6", s"N° Reg Inicial = $NumReg", NumReg == 6)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
        
      Control.NewStep("PASO 1: INSERTA NUEVOS")
      if (!TablaMaster.executeOnlyInsert("DF_Final_Todo", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "Si hay error en masterización", "Si hay error en masterización", s"Si hay error en masterización", p_testPlan_IsOK = true)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)     
      } else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "No hay error en masterización", "No hay error en masterización", s"No hay error en masterización", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
      }
      TablaMaster.DataFramehuemul.DataFrame.show()
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Masterización", "Si hay error en masterización", "Si hay error en masterización 1005", s"Si hay error en masterización (${TablaMaster.Error_Code})", TablaMaster.Error_Code == 1005)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
      
      val DFValidaCantFin = huemulLib.DF_ExecuteQuery("validaCantidad", s"select cast(count(1) as Long) as Cantidad from ${TablaMaster.getTable}")
      val NumRegFin = DFValidaCantFin.first().getAs[Long]("Cantidad")
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "NumRegFinal", "N° Registros Finales de tablas", "N° Reg Finales = 6", s"N° Reg Finales = $NumRegFin", NumRegFin == 6)
      Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
      
          Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxNewRecords_Num", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}