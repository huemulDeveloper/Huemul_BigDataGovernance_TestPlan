package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._


class tbl_DatosBasicosUpdate(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta (carga 1 vez, luego solo actualiza datos)")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  this.whoCanRunExecuteFullAddAccess("Proc_PlanPruebas_OnlyUpdate","com.huemulsolutions.bigdata.test")
  this.whoCanRunExecuteOnlyUpdateAddAccess("Proc_PlanPruebas_OnlyUpdate","com.huemulsolutions.bigdata.test")
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  
  val TipoValor = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( )
  TipoValor.setDqMinLen ( 2)
  TipoValor.setDqMaxLen ( 50)
  TipoValor.setNullable ( )
  
  
  val IntValue = new HuemulColumns(IntegerType,true,"datos integer")
  IntValue.setNullable ( )
  IntValue.setMdmEnableDTLog ( )
  IntValue.setMdmEnableOldValue ( )
  IntValue.setMdmEnableProcessLog ( )
  IntValue.setMdmEnableOldValueFullTrace( )
  
  val BigIntValue = new HuemulColumns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  BigIntValue.setMdmEnableDTLog ( )
  BigIntValue.setMdmEnableOldValue ( )
  BigIntValue.setMdmEnableProcessLog ( )
  BigIntValue.setMdmEnableOldValueFullTrace( )
  
  val SmallIntValue = new HuemulColumns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( )
  SmallIntValue.setMdmEnableDTLog ( )
  SmallIntValue.setMdmEnableOldValue ( )
  SmallIntValue.setMdmEnableProcessLog ( )
  SmallIntValue.setMdmEnableOldValueFullTrace( )
  
  val TinyIntValue = new HuemulColumns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( )
  TinyIntValue.setMdmEnableDTLog ( )
  TinyIntValue.setMdmEnableOldValue ( )
  TinyIntValue.setMdmEnableProcessLog ( )
  TinyIntValue.setMdmEnableOldValueFullTrace( )
  
  val DecimalValue = new HuemulColumns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( )
  DecimalValue.setMdmEnableDTLog ( )
  DecimalValue.setMdmEnableOldValue ( )
  DecimalValue.setMdmEnableProcessLog ( )
  DecimalValue.setMdmEnableOldValueFullTrace( )
  
  val RealValue = new HuemulColumns(DoubleType,true,"datos Real")
  RealValue.setNullable ( )
  RealValue.setMdmEnableDTLog ( )
  RealValue.setMdmEnableOldValue ( )
  RealValue.setMdmEnableProcessLog ( )
  RealValue.setMdmEnableOldValueFullTrace( )
  
  val FloatValue = new HuemulColumns(FloatType,true,"datos Float")
  FloatValue.setNullable ( )
  FloatValue.setMdmEnableDTLog ( )
  FloatValue.setMdmEnableOldValue ( )
  FloatValue.setMdmEnableProcessLog ( )
  
  val StringValue = new HuemulColumns(StringType,true,"datos String")
  StringValue.setNullable ( )
  StringValue.setMdmEnableDTLog ( )
  StringValue.setMdmEnableOldValue ( )
  StringValue.setMdmEnableProcessLog ( )
  StringValue.setMdmEnableOldValueFullTrace( )
  
  val charValue = new HuemulColumns(StringType,true,"datos Char")
  charValue.setNullable ( )
  charValue.setMdmEnableDTLog ( )
  charValue.setMdmEnableOldValue ( )
  charValue.setMdmEnableProcessLog ( )
  
  val timeStampValue = new HuemulColumns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ( )
  timeStampValue.setMdmEnableDTLog ( )
  timeStampValue.setMdmEnableOldValue ( )
  timeStampValue.setMdmEnableProcessLog ( )
  timeStampValue.setMdmEnableOldValueFullTrace( )
  
  val StringNoModificarValue = new HuemulColumns(StringType,true,"datos String solo inserta, no modifica")
  StringNoModificarValue.setNullable ( )

  
  
  val IntDefaultValue = new HuemulColumns(IntegerType,false,"datos default integer")
  IntDefaultValue.setDefaultValues ( "10000")
  
  val BigIntDefaultValue = new HuemulColumns(LongType,false,"datos default BigInt")
  BigIntDefaultValue.setDefaultValues ( "10000")
  
  val SmallIntDefaultValue = new HuemulColumns(ShortType,false,"datos default SmallInt")
  SmallIntDefaultValue.setDefaultValues ( "10000")
  
  val TinyIntDefaultValue = new HuemulColumns(ShortType,false,"datos default TinyInt")
  TinyIntDefaultValue.setDefaultValues ( "10000")
  
  val DecimalDefaultValue = new HuemulColumns(DecimalType(10,4),false,"datos default Decimal(10,4)")
  DecimalDefaultValue.setDefaultValues ( "10000.345")
  
  val RealDefaultValue = new HuemulColumns(DoubleType,false,"datos default Real")
  RealDefaultValue.setDefaultValues ( "10000.456")
  
  val FloatDefaultValue = new HuemulColumns(FloatType,false,"datos default Float")
  FloatDefaultValue.setDefaultValues ( "10000.567")
  
  val StringDefaultValue = new HuemulColumns(StringType,false,"datos default String")
  StringDefaultValue.setDefaultValues ( "'valor en string'")
  
  val charDefaultValue = new HuemulColumns(StringType,false,"datos default Char")
  charDefaultValue.setDefaultValues ( "cast('hola' as string)")
  
  val timeStampDefaultValue = new HuemulColumns(TimestampType,false,"datos default TimeStamp")
  timeStampDefaultValue.setDefaultValues ( "'2019-01-01'")
  
  
  this.applyTableDefinition()
  
}