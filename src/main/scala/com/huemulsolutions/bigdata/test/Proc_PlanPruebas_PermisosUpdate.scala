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
object Proc_PlanPruebas_PermisosUpdate {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - getWhoCanRun_executeOnlyUpdate",args,com.yourcompany.settings.globalSettings.global)
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
      TablaMaster.whoCanRunExecuteOnlyInsertAddAccess("agrega otro", "cualquier clase")
  
      
      IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyUpdate","Pudo agregar acceso", "no Pudo agregar acceso", s"Pudo agregar acceso", p_testPlan_IsOK = false)
      Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyUpdate", IdTestPlan)
      
      
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //  I N I C I A   P L A N   D E   P R U E B A S
      /////////////////////////////////////////////////////////////////////////////////////////
      /////////////////////////////////////////////////////////////////////////////////////////
      //valida que respuesta sea negativa
      
      Control.finishProcessOk
    } catch {
      case e: Exception => 
        
        val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "getWhoCanRun_executeOnlyUpdate", "ERROR DE PROGRAMA -  deberia tener errror", "con error 1033", s"con error: ${Control.controlError.controlErrorErrorCode}", Control.controlError.controlErrorErrorCode == 1033)
        Control.RegisterTestPlanFeature("getWhoCanRun_executeOnlyUpdate", IdTestPlan)
        Control.controlError.setError(e, this.getClass.getSimpleName, 1)
        Control.finishProcessError()
    }
    
     if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
      
    huemulLib.close()
  }
}