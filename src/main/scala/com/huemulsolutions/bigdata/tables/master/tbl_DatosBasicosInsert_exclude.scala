package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._


class tbl_DatosBasicosInsert_exclude(HuemulLib: huemul_BigDataGovernance, Control: huemul_Control, TipoTabla: huemulType_StorageType) extends huemul_Table(HuemulLib,Control) with Serializable {
  this.setTableType(huemulType_Tables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta (carga 1 vez, luego solo inserta datos)")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(TipoTabla)
  //this.setStorageType(huemulType_StorageType.PARQUET)
  this.setFrequency(huemulType_Frequency.ANY_MOMENT)
  
    //Agrega version 1.3
  this.setNumPartitions(2)
  
  //Agrega versión 2.0
  this.setSaveBackup(true)

  
  val TipoValor = new huemul_Columns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( )
  TipoValor.setDQ_MinLen ( 2)
  TipoValor.setDQ_MaxLen ( 50)
  
  
  val IntValue = new huemul_Columns(IntegerType,true,"datos integer")
  IntValue.setNullable ( )
  IntValue.setMDM_EnableDTLog ( )
  IntValue.setMDM_EnableOldValue ( )
  IntValue.setMDM_EnableProcessLog ( )
  
  val BigIntValue = new huemul_Columns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  BigIntValue.setMDM_EnableDTLog ( )
  BigIntValue.setMDM_EnableOldValue ( )
  BigIntValue.setMDM_EnableProcessLog ( )
  
  val SmallIntValue = new huemul_Columns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( )
  SmallIntValue.setMDM_EnableDTLog ( )
  SmallIntValue.setMDM_EnableOldValue ( )
  SmallIntValue.setMDM_EnableProcessLog ( )
  
  val TinyIntValue = new huemul_Columns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( )
  TinyIntValue.setMDM_EnableDTLog ( )
  TinyIntValue.setMDM_EnableOldValue ( )
  TinyIntValue.setMDM_EnableProcessLog ( )
  
  val DecimalValue = new huemul_Columns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( )
  DecimalValue.setMDM_EnableDTLog ( )
  DecimalValue.setMDM_EnableOldValue ( )
  DecimalValue.setMDM_EnableProcessLog ( )
  
  val RealValue = new huemul_Columns(DoubleType,true,"datos Real")
  RealValue.setNullable ( )
  RealValue.setMDM_EnableDTLog ( )
  RealValue.setMDM_EnableOldValue ( )
  RealValue.setMDM_EnableProcessLog ( )
  
  val FloatValue = new huemul_Columns(FloatType,true,"datos Float")
  FloatValue.setNullable ( )
  FloatValue.setMDM_EnableDTLog ( )
  FloatValue.setMDM_EnableOldValue ( )
  FloatValue.setMDM_EnableProcessLog ( )
  
  val StringValue = new huemul_Columns(StringType,true,"datos String")
  StringValue.setNullable ( )
  StringValue.setMDM_EnableDTLog ( )
  StringValue.setMDM_EnableOldValue ( )
  StringValue.setMDM_EnableProcessLog ( )
  
  val charValue = new huemul_Columns(StringType,true,"datos Char")
  charValue.setNullable ( )
  charValue.setMDM_EnableDTLog ( )
  charValue.setMDM_EnableOldValue ( )
  charValue.setMDM_EnableProcessLog ( )
  
  val timeStampValue = new huemul_Columns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ( )
  timeStampValue.setMDM_EnableDTLog ( )
  timeStampValue.setMDM_EnableOldValue ( )
  timeStampValue.setMDM_EnableProcessLog ( )
  
  
   val IntDefaultValue = new huemul_Columns(IntegerType,false,"datos default integer")
  IntDefaultValue.setDefaultValues ( "10000")
  
  val BigIntDefaultValue = new huemul_Columns(LongType,false,"datos default BigInt")
  BigIntDefaultValue.setDefaultValues ( "10000")
  
  val SmallIntDefaultValue = new huemul_Columns(ShortType,false,"datos default SmallInt")
  SmallIntDefaultValue.setDefaultValues ( "10000")
  
  val TinyIntDefaultValue = new huemul_Columns(ShortType,false,"datos default TinyInt")
  TinyIntDefaultValue.setDefaultValues ( "10000")
  
  val DecimalDefaultValue = new huemul_Columns(DecimalType(10,4),false,"datos default Decimal(10,4)")
  DecimalDefaultValue.setDefaultValues ( "10000.345")
  
  val RealDefaultValue = new huemul_Columns(DoubleType,false,"datos default Real")
  RealDefaultValue.setDefaultValues ( "10000.456")
  
  val FloatDefaultValue = new huemul_Columns(FloatType,false,"datos default Float")
  FloatDefaultValue.setDefaultValues ( "10000.567")
  
  val StringDefaultValue = new huemul_Columns(StringType,false,"datos default String")
  StringDefaultValue.setDefaultValues ( "'valor en string'")
  
  val charDefaultValue = new huemul_Columns(StringType,false,"datos default Char")
  charDefaultValue.setDefaultValues ( "cast('hola' as string)")
  
  val timeStampDefaultValue = new huemul_Columns(TimestampType,false,"datos default TimeStamp")
  timeStampDefaultValue.setDefaultValues ( "'2019-01-01'")
  
  //Regla para probar exclusión de registro al fallar un warning
  val DQ_warning_exclude: huemul_DataQuality = new huemul_DataQuality(TipoValor ,"Exclusión de valor Cero-Vacio", "tipoValor not in ('Cero-Vacio')",1).setNotification(huemulType_DQNotification.WARNING_EXCLUDE).setQueryLevel(huemulType_DQQueryLevel.Row)
  val DQ_warning_solo: huemul_DataQuality = new huemul_DataQuality(TipoValor ,"Solo warning cuando aparezca registro Cero-Vacio", "tipoValor <> 'Negativo_Maximo'",2).setNotification(huemulType_DQNotification.WARNING).setQueryLevel(huemulType_DQQueryLevel.Row)
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicosNuevos = new tbl_DatosBasicosNuevos(HuemulLib,Control, TipoTabla)
  val fk_tbl_DatosBasicos: huemul_Table_Relationship = new huemul_Table_Relationship(itbl_DatosBasicosNuevos, false).setNotification(huemulType_DQNotification.WARNING_EXCLUDE).broadcastJoin(true)
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicosNuevos.TipoValor , TipoValor)
  
  
  this.ApplyTableDefinition()
  
}