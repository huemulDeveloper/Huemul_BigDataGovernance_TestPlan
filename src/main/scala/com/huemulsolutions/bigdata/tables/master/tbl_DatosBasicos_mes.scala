package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicos_mes(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Transaction)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  if (TipoTabla == HuemulTypeStorageType.HBASE)
    this.setStorageType(HuemulTypeStorageType.PARQUET)
  else 
    this.setStorageType(TipoTabla)
  
  this.setDQ_MaxNewRecords_Num(4)
  //this.setPartitionField("periodo_mes")
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  val periodo_mes = new HuemulColumns(StringType,true,"periodo")
  periodo_mes.setIsPK ( )
  periodo_mes.setPartitionColumn(1)

  
  val TipoValor: HuemulColumns = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
    .setIsPK ()
    .setDQ_MinLen ( 2,null)
    .setDQ_MaxLen ( 50,null)
  
  
  val IntValue: HuemulColumns = new HuemulColumns(IntegerType,true,"datos integer")
      .setNullable ( )
  
  
  
  val BigIntValue = new HuemulColumns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  
  val SmallIntValue = new HuemulColumns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ()
  
  val TinyIntValue = new HuemulColumns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ()
  
  val DecimalValue = new HuemulColumns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ()
  
  val RealValue = new HuemulColumns(DoubleType,true,"datos Real")
  RealValue.setNullable ()
  
  val FloatValue = new HuemulColumns(FloatType,true,"datos Float")
  FloatValue.setNullable ()
  
  val StringValue = new HuemulColumns(StringType,true,"datos String")
  StringValue.setNullable ()
  
  val charValue = new HuemulColumns(StringType,true,"datos Char")
  charValue.setNullable ()
  
  val timeStampValue = new HuemulColumns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ()
  
  
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicos = new tbl_DatosBasicos(HuemulLib,Control)
  val fk_tbl_DatosBasicos = new HuemulTableRelationship(itbl_DatosBasicos, false)
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicos.TipoValor , TipoValor)
  
  
  this.ApplyTableDefinition()
  
}