package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._


class tbl_DatosBasicosUpdate(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta (carga 1 vez, luego solo actualiza datos)")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  this.WhoCanRun_executeFull_addAccess("Proc_PlanPruebas_OnlyUpdate","com.huemulsolutions.bigdata.test")
  this.WhoCanRun_executeOnlyUpdate_addAccess("Proc_PlanPruebas_OnlyUpdate","com.huemulsolutions.bigdata.test")
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  
  val TipoValor = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( )
  TipoValor.setDQ_MinLen ( 2)
  TipoValor.setDQ_MaxLen ( 50)
  TipoValor.setNullable ( )
  
  
  val IntValue = new HuemulColumns(IntegerType,true,"datos integer")
  IntValue.setNullable ( )
  IntValue.setMDM_EnableDTLog ( )
  IntValue.setMDM_EnableOldValue ( )
  IntValue.setMDM_EnableProcessLog ( )
  IntValue.setMDM_EnableOldValue_FullTrace( )
  
  val BigIntValue = new HuemulColumns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  BigIntValue.setMDM_EnableDTLog ( )
  BigIntValue.setMDM_EnableOldValue ( )
  BigIntValue.setMDM_EnableProcessLog ( )
  BigIntValue.setMDM_EnableOldValue_FullTrace( )
  
  val SmallIntValue = new HuemulColumns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( )
  SmallIntValue.setMDM_EnableDTLog ( )
  SmallIntValue.setMDM_EnableOldValue ( )
  SmallIntValue.setMDM_EnableProcessLog ( )
  SmallIntValue.setMDM_EnableOldValue_FullTrace( )
  
  val TinyIntValue = new HuemulColumns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( )
  TinyIntValue.setMDM_EnableDTLog ( )
  TinyIntValue.setMDM_EnableOldValue ( )
  TinyIntValue.setMDM_EnableProcessLog ( )
  TinyIntValue.setMDM_EnableOldValue_FullTrace( )
  
  val DecimalValue = new HuemulColumns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( )
  DecimalValue.setMDM_EnableDTLog ( )
  DecimalValue.setMDM_EnableOldValue ( )
  DecimalValue.setMDM_EnableProcessLog ( )
  DecimalValue.setMDM_EnableOldValue_FullTrace( )
  
  val RealValue = new HuemulColumns(DoubleType,true,"datos Real")
  RealValue.setNullable ( )
  RealValue.setMDM_EnableDTLog ( )
  RealValue.setMDM_EnableOldValue ( )
  RealValue.setMDM_EnableProcessLog ( )
  RealValue.setMDM_EnableOldValue_FullTrace( )
  
  val FloatValue = new HuemulColumns(FloatType,true,"datos Float")
  FloatValue.setNullable ( )
  FloatValue.setMDM_EnableDTLog ( )
  FloatValue.setMDM_EnableOldValue ( )
  FloatValue.setMDM_EnableProcessLog ( )
  
  val StringValue = new HuemulColumns(StringType,true,"datos String")
  StringValue.setNullable ( )
  StringValue.setMDM_EnableDTLog ( )
  StringValue.setMDM_EnableOldValue ( )
  StringValue.setMDM_EnableProcessLog ( )
  StringValue.setMDM_EnableOldValue_FullTrace( )
  
  val charValue = new HuemulColumns(StringType,true,"datos Char")
  charValue.setNullable ( )
  charValue.setMDM_EnableDTLog ( )
  charValue.setMDM_EnableOldValue ( )
  charValue.setMDM_EnableProcessLog ( )
  
  val timeStampValue = new HuemulColumns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ( )
  timeStampValue.setMDM_EnableDTLog ( )
  timeStampValue.setMDM_EnableOldValue ( )
  timeStampValue.setMDM_EnableProcessLog ( )
  timeStampValue.setMDM_EnableOldValue_FullTrace( )
  
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
  
  
  this.ApplyTableDefinition()
  
}