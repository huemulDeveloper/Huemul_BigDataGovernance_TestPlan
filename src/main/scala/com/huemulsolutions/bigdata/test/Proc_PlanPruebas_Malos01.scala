package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos


/**
 * Este plan de pruebas valida lo siguiente:
 * error en PK: hay registros duplicados, lo que se espera es un error de PK
 * el TipodeArchivo usado es Malo01
 */
object Proc_PlanPruebas_Malos01 {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas Malos01",args,com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
    
    val Ano = huemulLib.arguments.getValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.getValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    var IdTestPlan: String = ""
    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.newStep("Define dataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"Malos01")) {
        Control.raiseError(s"error al intentar abrir archivo de datos: ${DF_RAW.error.controlErrorMessage}")
      }
      Control.newStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicos(huemulLib, Control)      
      TablaMaster.dfFromDf(DF_RAW.dataFrameHuemul.dataFrame, "DF_RAW", "DF_Original")
      
      //TablaMaster.dfFromSql("DF_Original", "select * from DF_RAW")
      
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
      
      val ValorexecuteFull = TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Malo01 - error PK", "El proceso debe retornar false", "ValorexecuteFull = false", s"ValorexecuteFull = $ValorexecuteFull", !ValorexecuteFull)
      Control.RegisterTestPlanFeature("IsPK error", IdTestPlan)
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Malo01 - error PK 1018", "El proceso debe retornar cod. error 1018", "error_code=1018", s"error_code= ${TablaMaster.errorCode}", TablaMaster.errorCode == 1018)
      Control.RegisterTestPlanFeature("IsPK error", IdTestPlan)
      
      //valida que N° de registros con problemas de PK = 1
      val NumErrores_TipoValor = TablaMaster.dataFrameHuemul.getDqResult.filter { x => x.dqErrorCode == 1018 }
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Malo01 - Obtiene DQ PK", "N° registros devueltos de DQ pK = 1", "N° Reg = 1", s"N° Reg = ${NumErrores_TipoValor.length}", NumErrores_TipoValor.length == 1)
      Control.RegisterTestPlanFeature("IsPK error", IdTestPlan)
      
      
      val errores2 = huemulLib.spark.sql(s"""select dq_error_columnname
                                                  ,cast(count(1) as int) as Cantidad
                                                  ,cast(sum(case when tipovalor = "Positivo_Maximo" then 1 else 0 end) as Int) as error_01
                        from production_dqerror.tbl_DatosBasicos_dq 
                        where dq_control_id = '${Control.Control_Id}' 
                        and dq_error_code = 1018
                        group by dq_error_columnname """).collect
      val cantidad = errores2.filter { x => x.getAs[String]("dq_error_columnname") == "TipoValor" }(0).getAs[Int]("Cantidad") 
      val error_01 = errores2.filter { x => x.getAs[String]("dq_error_columnname") == "TipoValor" }(0).getAs[Int]("error_01") 
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Guarda errores en tabla _dq TipoValor", "errores en columna TipoValor", "Cantidad con errores = 2", s"Cantidad con errores = $cantidad", cantidad == 2)
      Control.RegisterTestPlanFeature("FK error encontrado", IdTestPlan)
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "error encontrado (tipovalor = Positivo_Maximo)", "error encontrado (tipovalor = Positivo_Maximo)", "error_01 = 2", s"error_01 = $error_01", error_01 == 2)
      Control.RegisterTestPlanFeature("FK error encontrado", IdTestPlan)
      
      
      /* //esta prueba fue comentada, antes entregaba el N° de registros con error, ahora no
       * //esta será una modificación de versión 1.2
      var PK_NumRowsTotal: Long = 0
      var PK_NumRowsError: Long = -1
      if (NumErrores_TipoValor.length == 1) {
        PK_NumRowsTotal = NumErrores_TipoValor(0).dqNumRowsTotal
        PK_NumRowsError = NumErrores_TipoValor(0).dqNumRowsError
      }
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "Malo01 - N° PK duplicados", "N° registros duplicados, debe ser 1", "N° PK Duplicados = 1", s"N° PK Duplicados = ${PK_NumRowsError}", PK_NumRowsError == 1)
      Control.RegisterTestPlanFeature("IsPK error", IdTestPlan)
      */
      
      TablaMaster.dataFrameHuemul.getDqResult.foreach { x =>
        println(s"dqName:${x.dqName}, bbddName:${x.bbddName}, tableName:${x.tableName}, columnName:${x.columnName}, dqNumRowsTotal:${x.dqNumRowsTotal}, dqNumRowsOk:${x.dqNumRowsOk}, dqNumRowsError:${x.dqNumRowsError}")
      }
      
      /**************  P L A N   D E   P R U E B A S  *********************/
      
      
      
      
      
      Control.finishProcessOk
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("IsPK error", IdTestPlan)
        Control.controlError.setError(e, this.getClass.getSimpleName, 1)
        Control.finishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}