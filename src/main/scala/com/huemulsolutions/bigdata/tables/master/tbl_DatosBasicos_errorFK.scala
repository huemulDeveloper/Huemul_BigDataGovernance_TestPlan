package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import org.apache.spark.sql.types.DataTypes._



class tbl_DatosBasicos_errorFK(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que error en FK funcione con registro en DQ")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setDQ_MaxNewRecords_Num(4)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)

  val Codigo = new HuemulColumns(IntegerType,true,"Codigo del registro PK")
  Codigo.setIsPK ( )
  
  
  val TipoValor: HuemulColumns = new HuemulColumns(StringType,true,"Nombre del tipo de valor (FK)")
      .setDQ_MinLen ( 2,null)
      .setDQ_MaxLen ( 50,null)
      
  
  val CampoAdicional = new HuemulColumns(StringType,false,"valor Adicional")
  CampoAdicional.setDefaultValues("'no asignado'")
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicos = new tbl_DatosBasicos(HuemulLib,Control)
  val fk_tbl_DatosBasicos: HuemulTableRelationship = new HuemulTableRelationship(itbl_DatosBasicos, false).setExternalCode("USER_FK_CODE")
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicos.TipoValor , TipoValor)
  
  
  
  this.ApplyTableDefinition()
  
}