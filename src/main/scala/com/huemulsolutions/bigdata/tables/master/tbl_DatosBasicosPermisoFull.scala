package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicosPermisoFull(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setDQMaxNewRecordsNum(4)
  this.whoCanRunExecuteFullAddAccess("com.huemulsolutions.bigdata.test", "Proc_PlanPruebas_PermisosFull")
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)

  
  val TipoValor = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( )
  TipoValor.setDqMinLen ( 2)
  TipoValor.setDqMaxLen ( 50)
  
  
  val IntValue = new HuemulColumns(IntegerType,true,"datos integer")
  IntValue.setNullable ( )
  
  
  
  val BigIntValue = new HuemulColumns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  
  val SmallIntValue = new HuemulColumns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( )
  
  val TinyIntValue = new HuemulColumns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( )
  
  val DecimalValue = new HuemulColumns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( )
  
  val RealValue = new HuemulColumns(DoubleType,true,"datos Real")
  RealValue.setNullable ( )
  
  val FloatValue = new HuemulColumns(FloatType,true,"datos Float")
  FloatValue.setNullable ( )
  
  val StringValue = new HuemulColumns(StringType,true,"datos String")
  StringValue.setNullable ( )
  
  val charValue = new HuemulColumns(StringType,true,"datos Char")
  charValue.setNullable ( )
  
  val timeStampValue = new HuemulColumns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ( )
  
  
  
  
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