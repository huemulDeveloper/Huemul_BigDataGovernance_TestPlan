package com.huemulsolutions.bigdata.test

import com.huemulsolutions.bigdata.control.HuemulControl
import com.huemulsolutions.bigdata.control.HuemulTypeFrequency
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance

object dropOldBackups {
  def main(args: Array[String]): Unit = {
 
    val huemulBigDataGov  = new HuemulBigDataGovernance(s"BigDataGovernance Util", args, com.yourcompany.settings.globalSettings.Global)
    
    //numBackupToMaintain
    val numBackupToMaintain = huemulBigDataGov.arguments.getValue("numBackupToMaintain", null,"param missing: numBackupToMaintain. Example: numBackupToMaintain=2 to maintain last 2 backups")
    
    if (numBackupToMaintain != null) {
      val Control = new HuemulControl(huemulBigDataGov,null, HuemulTypeFrequency.ANY_MOMENT, false, true)
      Control.control_getBackupToDelete(numBackupToMaintain.toInt)
    }
      
    huemulBigDataGov.close()
    //Control.Init_CreateTables()
  }
}
