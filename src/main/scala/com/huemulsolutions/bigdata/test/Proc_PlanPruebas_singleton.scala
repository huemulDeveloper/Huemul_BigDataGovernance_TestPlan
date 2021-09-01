package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._


object Proc_PlanPruebas_singleton {
  def main(args: Array[String]): Unit = {
    val huemulLib = new HuemulBigDataGovernance("01 - Plan pruebas Proc_PlanPruebas_singleton",args,com.yourcompany.settings.globalSettings.Global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)

    while (true) {
      print("app viva, haciendo esperar a otra app en modo singleton, para continuar tienen que matarme")
      Thread.sleep(10000)
    }
    
    if (Control.TestPlan_CurrentIsOK(null))
      println("Proceso OK")
    
    huemulLib.close()
  }
}