package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicosErrores
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTableConnector
import com.huemulsolutions.bigdata.tables.HuemulTypeInternalTableType

object Proc_PlanPruebas_Errores {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_CargaMaster",args,com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
    
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
      Control.newStep("Define dataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.raiseError(s"error al intentar abrir archivo de datos: ${DF_RAW.error.controlErrorMessage}")
      }
      Control.newStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicosErrores(huemulLib, Control,TipoTabla)      
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
        
      if (TipoTablaParam == "hbase") {
        Control.newStep("borrar tabla")
        val th = new HuemulTableConnector(huemulLib, Control)
        th.tableDeleteHBase(TablaMaster.getHBaseNamespace(HuemulTypeInternalTableType.Normal), TablaMaster.getHBaseTableName(HuemulTypeInternalTableType.Normal))
      }
        
   //BORRA HDFS ANTIGUO PARA EFECTOS DEL PLAN DE PRUEBAS
        
      TablaMaster.TipoValor.setMapping("TipoValor",replaceValueOnUpdate = true,"coalesce(new.TipoValor,'nulo')","coalesce(new.TipoValor,'nulo')")
      TablaMaster.Column_DQ_MaxDateTimeValue.setMapping("timeStampValue")
      TablaMaster.Column_DQ_MaxDecimalValue.setMapping("DecimalValue")
      TablaMaster.Column_DQ_MaxLen.setMapping("StringValue")
      TablaMaster.Column_DQ_MinDateTimeValue.setMapping("timeStampValue")
      TablaMaster.Column_DQ_MinDecimalValue.setMapping("DecimalValue")
      TablaMaster.Column_DQ_MinLen.setMapping("StringValue")
      TablaMaster.Column_IsUnique.setMapping("StringValue")
      TablaMaster.Column_NoMapeadoDefault.setMapping("")
      TablaMaster.Column_NotNull.setMapping("IntValue")
      
      
      huemulLib.spark.sql("select StringValue, length(StringValue) as largo, case when StringValue is null then 1 else 0 end esNulo from DF_Original").show()
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.newStep("Ejecución")
      TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY_SER)
      var IdTestPlan: String = ""
      //Column_DQ_MinLen
      var ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_DQ_MinLen".toLowerCase() && x.dqErrorCode == 1020 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinLen", "No hay error encontrado", "ErrorCode = 1020", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MinLen", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinLen Codigo", "Errores encontrados correctamente", "ErrorCode = 1020, N° registros con error: 3", s"ErrorCode = 1020, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 3)
        Control.RegisterTestPlanFeature("DQ_MinLen", IdTestPlan)
      }
      
      //Column_DQ_MaxLen
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_DQ_MaxLen".toLowerCase() && x.dqErrorCode == 1020 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxLen", "No hay error encontrado", "ErrorCode = 1020", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxLen", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxLen Codigo", "Errores encontrados correctamente", "ErrorCode = 1020, N° registros con error: 2", s"ErrorCode = 1020, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 2)
        Control.RegisterTestPlanFeature("DQ_MaxLen", IdTestPlan)
      }
      
      //Column_DQ_MinDecimalValue
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_DQ_MinDecimalValue".toLowerCase() && x.dqErrorCode == 1021 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDecimalValue", "No hay error encontrado", "ErrorCode = 1021", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MinDecimalValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDecimalValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1021, N° registros con error: 2", s"ErrorCode = 1021, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 2)
        Control.RegisterTestPlanFeature("DQ_MinDecimalValue", IdTestPlan)
      }
      
      //Column_DQ_MaxDecimalValue
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_DQ_MaxDecimalValue".toLowerCase() && x.dqErrorCode == 1021 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDecimalValue", "No hay error encontrado", "ErrorCode = 1021", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxDecimalValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDecimalValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1021, N° registros con error: 1", s"ErrorCode = 1021, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 1)
        Control.RegisterTestPlanFeature("DQ_MaxDecimalValue", IdTestPlan)
      }
      
      //Column_DQ_MinDateTimeValue
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_DQ_MinDateTimeValue".toLowerCase() && x.dqErrorCode == 1022 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDateTimeValue", "No hay error encontrado", "ErrorCode = 1022", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MinDateTimeValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MinDateTimeValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1022, N° registros con error: 3", s"ErrorCode = 1022, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 3)
        Control.RegisterTestPlanFeature("DQ_MinDateTimeValue", IdTestPlan)
      }
      
      //Column_DQ_MaxDateTimeValue
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_DQ_MaxDateTimeValue".toLowerCase() && x.dqErrorCode == 1022 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDateTimeValue", "No hay error encontrado", "ErrorCode = 1022", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("DQ_MaxDateTimeValue", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_DQ_MaxDateTimeValue Codigo", "Errores encontrados correctamente", "ErrorCode = 1022, N° registros con error: 2", s"ErrorCode = 1022, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 2)
        Control.RegisterTestPlanFeature("DQ_MaxDateTimeValue", IdTestPlan)
      }
      
      //Column_NotNull
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_NotNull".toLowerCase() && x.dqErrorCode == 1023 }
      if (ErrorReg == null || ErrorReg.isEmpty) {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_NotNull", "No hay error encontrado", "ErrorCode = 1023", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("Nullable", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_NotNull Codigo", "Errores encontrados correctamente", "ErrorCode = 1023, N° registros con error: 1", s"ErrorCode = 1023, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 1)
        Control.RegisterTestPlanFeature("Nullable", IdTestPlan)
      }
      
      //Column_IsUnique
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.columnName != null && x.columnName.toLowerCase() == "Column_IsUnique".toLowerCase() && x.dqErrorCode == 2006 }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_IsUnique", "No hay error encontrado", "ErrorCode = 2006", s"SIN REGISTRO", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("IsUnique", IdTestPlan)
      }
      else {
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_IsUnique Codigo", "Errores encontrados correctamente", "ErrorCode = 2006, N° errores = 1", s"ErrorCode = 2006, N° errores = ${ErrorReg.length}", ErrorReg.length == 1)
        Control.RegisterTestPlanFeature("IsUnique", IdTestPlan)
        /*
         * comentado, descomentar cuando se agregue la captura de los registros que no fueron únicos.
        val ErrorEncontrado = ErrorReg(0)
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR Column_IsUnique Codigo", "Errores encontrados correctamente", "ErrorCode = 2006, N° registros Not Unique: 2", s"ErrorCode = 2006, N° registros con error: ${ErrorEncontrado.dqNumRowsError}", ErrorEncontrado.dqNumRowsError == 2)
        Control.RegisterTestPlanFeature("IsUnique", IdTestPlan)
        * 
        */
      }
      
      //N° Total de errores por DQ debe ser = 8
      ErrorReg = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.dqIsError }
      if (ErrorReg == null || ErrorReg.isEmpty){
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR N° DQ fallados", "No hay error encontrado", "Cantidad de errores DQ = 8", s"Cantidad de errores DQ = ${ErrorReg.length}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      }
      else {
        val ErrorEncontrado = ErrorReg.length
        IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR N° DQ Fallados cod", "Errores encontrados correctamente", "Cantidad de errores DQ = 8", s"Cantidad de errores DQ = $ErrorEncontrado", ErrorEncontrado == 8)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
      }
      
      println(s"detalle del 100% de DQ aplicado en esta prueba")
      TablaMaster.dataFrameHuemul.getDqResult.foreach { x =>
        println(s"dqName:[${x.dqName}], ErrorCode:${x.dqErrorCode} bbddName:${x.bbddName}, tableName:${x.tableName}, columnName:${x.columnName}, dqNumRowsTotal:${x.dqNumRowsTotal}, dqNumRowsOk:${x.dqNumRowsOk}, dqNumRowsError:${x.dqNumRowsError}")
      }
      
      val errores = huemulLib.spark.sql(s"select dq_error_columnname, cast(count(1) as int) as Cantidad from production_dqerror.tbl_datosbasicoserrores_dq where dq_control_id = '${Control.Control_Id}' group by dq_error_columnname ").collect
      var cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_DQ_MaxDateTimeValue".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MaxDateTimeValue", "errores en columna Column_DQ_MaxDateTimeValue", "Cantidad con errores = 2", s"Cantidad con errores = $cantidad", cantidad == 2)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)
      
      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_DQ_MaxDecimalValue".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MaxDecimalValue", "errores en columna Column_DQ_MaxDecimalValue", "Cantidad con errores = 1", s"Cantidad con errores = $cantidad", cantidad == 1)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)
      
      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_DQ_MaxLen".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MaxLen", "errores en columna Column_DQ_MaxLen", "Cantidad con errores = 2", s"Cantidad con errores = $cantidad", cantidad == 2)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_DQ_MinDateTimeValue".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MinDateTimeValue", "errores en columna Column_DQ_MinDateTimeValue", "Cantidad con errores = 3", s"Cantidad con errores = $cantidad", cantidad == 3)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_DQ_MinDecimalValue".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MinDecimalValue", "errores en columna Column_DQ_MinDecimalValue", "Cantidad con errores = 2", s"Cantidad con errores = $cantidad", cantidad == 2)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_DQ_MinLen".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_DQ_MinLen", "errores en columna Column_DQ_MinLen", "Cantidad con errores = 3", s"Cantidad con errores = $cantidad", cantidad == 3)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      cantidad = errores.filter { x => x.getAs[String]("dq_error_columnname").toLowerCase() == "Column_NotNull".toLowerCase() }(0).getAs[Int]("Cantidad") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq Column_NotNull", "errores en columna Column_NotNull", "Cantidad con errores = 1", s"Cantidad con errores = $cantidad", cantidad == 1)
      Control.RegisterTestPlanFeature("ControlErrores", IdTestPlan)

      
          Control.finishProcessOk
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("executeFull", IdTestPlan)
        Control.controlError.setError(e, this.getClass.getSimpleName, 1)
        Control.finishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(16))
      println("Proceso OK")
      
    huemulLib.close()
  }
}