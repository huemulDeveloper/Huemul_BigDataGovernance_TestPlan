package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.master.tbl_DatosBasicos
import com.huemulsolutions.bigdata.raw.raw_DatosBasicos


/**
 * Este plan de pruebas valida lo siguiente:
 * Error en mapear un campo que es requerido
 * 
 */
object Proc_PlanPruebas_NoMapped {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas Error Mapped",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
    
    val Ano = huemulLib.arguments.getValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017")
    val Mes = huemulLib.arguments.getValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12")
    
    val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
    var IdTestPlan: String = ""
    Control.AddParamInformation("TestPlanGroup", TestPlanGroup)
        
    try {
      Control.NewStep("Define DataFrame Original")
      val DF_RAW =  new raw_DatosBasicos(huemulLib, Control)
      if (!DF_RAW.open("DF_RAW", null, Ano.toInt, Mes.toInt, 1, 0, 0, 0,"")) {
        Control.RaiseError(s"Error al intentar abrir archivo de datos: ${DF_RAW.Error.ControlError_Message}")
      }
      Control.NewStep("Mapeo de Campos")
      val TablaMaster = new tbl_DatosBasicos(huemulLib, Control)      
      TablaMaster.DF_from_DF(DF_RAW.DataFramehuemul.DataFrame,"DF_RAW", "DF_Original")
      
      //TablaMaster.DF_from_SQL("DF_Original", "select * from DF_RAW")
      
      TablaMaster.TipoValor.setMapping("TipoValor")
      TablaMaster.IntValue.setMapping("IntValue")
      TablaMaster.BigIntValue.setMapping("BigIntValue")
      TablaMaster.SmallIntValue.setMapping("SmallIntValue")
      TablaMaster.TinyIntValue.setMapping("TinyIntValue")
      //TablaMaster.DecimalValue.setMapping("DecimalValue") --> este campo es obligatorio y no es mapeado
      TablaMaster.RealValue.setMapping("RealValue")
      TablaMaster.FloatValue.setMapping("FloatValue")
      TablaMaster.StringValue.setMapping("StringValue")
      TablaMaster.charValue.setMapping("charValue")
      TablaMaster.timeStampValue.setMapping("timeStampValue")
      //TODO: cambiar el parámetro "true" por algo.UPDATE O algo.NOUPDATE (en replaceValueOnUpdate
      Control.NewStep("Ejecución")
      
      val ValorexecuteFull = TablaMaster.executeFull("DF_Final", org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "NoMapped - Error de Mapeo", "El proceso debe retornar false", "ValorexecuteFull = false", s"ValorexecuteFull = $ValorexecuteFull", !ValorexecuteFull)
      Control.RegisterTestPlanFeature("MappedName", IdTestPlan)
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "NoMapped - Error Mapped 1016", "El proceso debe retornar cod. error 1016", "error_code=1016", s"error_code= ${TablaMaster.Error_Code}", TablaMaster.Error_Code == 1016)
      Control.RegisterTestPlanFeature("MappedName", IdTestPlan)
      
     
      TablaMaster.DataFramehuemul.getDQResult.foreach { x =>
        println(s"DQ_Name:${x.DQ_Name}, BBDD_Name:${x.BBDD_Name}, Table_Name:${x.Table_Name}, ColumnName:${x.ColumnName}, DQ_NumRowsTotal:${x.DQ_NumRowsTotal}, DQ_NumRowsOK:${x.DQ_NumRowsOK}, DQ_NumRowsError:${x.DQ_NumRowsError}") 
      }
      
      /**************  P L A N   D E   P R U E B A S  *********************/
      
      
      
      
      
      Control.FinishProcessOK
    } catch {
      case e: Exception => 
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "ERROR", "ERROR DE PROGRAMA -  no deberia tener errror", "sin error", s"con error: ${e.getMessage}", p_testPlan_IsOK = false)
        Control.RegisterTestPlanFeature("MappedName", IdTestPlan)
        Control.Control_Error.GetError(e, this.getClass.getSimpleName, 1)
        Control.FinishProcessError()
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}