package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import org.apache.spark.sql.types.DataTypes._



class tbl_OldValueTrace(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: verifica el correcto registro de los cambios en tabla oldvalue")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
 
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setDQMaxNewRecordsNum(4)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  this.whoCanRunExecuteFullAddAccess("Proc_PlanPruebas_OldValueTrace","com.huemulsolutions.bigdata.test")
  
  //Agrega version 1.3
  this.setNumPartitions(1)

  
  val codigo = new HuemulColumns(IntegerType,true,"Codigo")
  codigo.setIsPK ( )
  
  
  val Descripcion = new HuemulColumns(StringType,true,"descripción de la tabla")
  Descripcion.setNullable ( )
  Descripcion.setMdmEnableOldValueFullTrace( )
  
  val Fecha = new HuemulColumns(TimestampType,true,"datos TimeStamp")
  Fecha.setNullable ( )
  Fecha.setMdmEnableOldValueFullTrace( )
  Fecha.setMdmEnableDTLog()
  Fecha.setMdmEnableProcessLog()
  Fecha.setMdmEnableOldValue()
  
  val Monto = new HuemulColumns(IntegerType,true,"datos Monto")
  Monto.setNullable ( )
  Monto.setMdmEnableOldValueFullTrace( )
  Monto.setMdmEnableDTLog()
  Monto.setMdmEnableProcessLog()
  Monto.setMdmEnableOldValue()
   
  
  this.applyTableDefinition()
  
}