package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
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
  TipoValor.setIsPK ( true)
  TipoValor.setDQ_MinLen ( 2)
  TipoValor.setDQ_MaxLen ( 50)
  
  
  val IntValue = new huemul_Columns(IntegerType,true,"datos integer")
  IntValue.setNullable ( true)
  IntValue.setMDM_EnableDTLog ( true)
  IntValue.setMDM_EnableOldValue ( true)
  IntValue.setMDM_EnableProcessLog ( true)
  
  val BigIntValue = new huemul_Columns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( true)
  BigIntValue.setMDM_EnableDTLog ( true)
  BigIntValue.setMDM_EnableOldValue ( true)
  BigIntValue.setMDM_EnableProcessLog ( true)
  
  val SmallIntValue = new huemul_Columns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( true)
  SmallIntValue.setMDM_EnableDTLog ( true)
  SmallIntValue.setMDM_EnableOldValue ( true)
  SmallIntValue.setMDM_EnableProcessLog ( true)
  
  val TinyIntValue = new huemul_Columns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( true)
  TinyIntValue.setMDM_EnableDTLog ( true)
  TinyIntValue.setMDM_EnableOldValue ( true)
  TinyIntValue.setMDM_EnableProcessLog ( true)
  
  val DecimalValue = new huemul_Columns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( true)
  DecimalValue.setMDM_EnableDTLog ( true)
  DecimalValue.setMDM_EnableOldValue ( true)
  DecimalValue.setMDM_EnableProcessLog ( true)
  
  val RealValue = new huemul_Columns(DoubleType,true,"datos Real")
  RealValue.setNullable ( true)
  RealValue.setMDM_EnableDTLog ( true)
  RealValue.setMDM_EnableOldValue ( true)
  RealValue.setMDM_EnableProcessLog ( true)
  
  val FloatValue = new huemul_Columns(FloatType,true,"datos Float")
  FloatValue.setNullable ( true)
  FloatValue.setMDM_EnableDTLog ( true)
  FloatValue.setMDM_EnableOldValue ( true)
  FloatValue.setMDM_EnableProcessLog ( true)
  
  val StringValue = new huemul_Columns(StringType,true,"datos String")
  StringValue.setNullable ( true)
  StringValue.setMDM_EnableDTLog ( true)
  StringValue.setMDM_EnableOldValue ( true)
  StringValue.setMDM_EnableProcessLog ( true)
  
  val charValue = new huemul_Columns(StringType,true,"datos Char")
  charValue.setNullable ( true)
  charValue.setMDM_EnableDTLog ( true)
  charValue.setMDM_EnableOldValue ( true)
  charValue.setMDM_EnableProcessLog ( true)
  
  val timeStampValue = new huemul_Columns(TimestampType,true,"datos TimeStamp")
  timeStampValue.setNullable ( true)
  timeStampValue.setMDM_EnableDTLog ( true)
  timeStampValue.setMDM_EnableOldValue ( true)
  timeStampValue.setMDM_EnableProcessLog ( true)
  
  
   val IntDefaultValue = new huemul_Columns(IntegerType,false,"datos default integer")
  IntDefaultValue.setDefaultValue ( "10000")
  
  val BigIntDefaultValue = new huemul_Columns(LongType,false,"datos default BigInt")
  BigIntDefaultValue.setDefaultValue ( "10000")
  
  val SmallIntDefaultValue = new huemul_Columns(ShortType,false,"datos default SmallInt")
  SmallIntDefaultValue.setDefaultValue ( "10000")
  
  val TinyIntDefaultValue = new huemul_Columns(ShortType,false,"datos default TinyInt")
  TinyIntDefaultValue.setDefaultValue ( "10000")
  
  val DecimalDefaultValue = new huemul_Columns(DecimalType(10,4),false,"datos default Decimal(10,4)")
  DecimalDefaultValue.setDefaultValue ( "10000.345")
  
  val RealDefaultValue = new huemul_Columns(DoubleType,false,"datos default Real")
  RealDefaultValue.setDefaultValue ( "10000.456")
  
  val FloatDefaultValue = new huemul_Columns(FloatType,false,"datos default Float")
  FloatDefaultValue.setDefaultValue ( "10000.567")
  
  val StringDefaultValue = new huemul_Columns(StringType,false,"datos default String")
  StringDefaultValue.setDefaultValue ( "'valor en string'")
  
  val charDefaultValue = new huemul_Columns(StringType,false,"datos default Char")
  charDefaultValue.setDefaultValue ( "cast('hola' as string)")
  
  val timeStampDefaultValue = new huemul_Columns(TimestampType,false,"datos default TimeStamp")
  timeStampDefaultValue.setDefaultValue ( "'2019-01-01'")
  
  //Regla para probar exclusión de registro al fallar un warning
  val DQ_warning_exclude: huemul_DataQuality = new huemul_DataQuality(TipoValor ,"Exclusión de valor Cero-Vacio", "tipoValor not in ('Cero-Vacio')",1).setNotification(huemulType_DQNotification.WARNING_EXCLUDE).setQueryLevel(huemulType_DQQueryLevel.Row)
  val DQ_warning_solo: huemul_DataQuality = new huemul_DataQuality(TipoValor ,"Solo warning cuando aparezca registro Cero-Vacio", "tipoValor <> 'Negativo_Maximo'",2).setNotification(huemulType_DQNotification.WARNING).setQueryLevel(huemulType_DQQueryLevel.Row)
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicosNuevos = new tbl_DatosBasicosNuevos(HuemulLib,Control, TipoTabla)
  val fk_tbl_DatosBasicos = new huemul_Table_Relationship(itbl_DatosBasicosNuevos, false).setNotification(huemulType_DQNotification.WARNING_EXCLUDE).broadcastJoin(true)
  fk_tbl_DatosBasicos.AddRelationship(itbl_DatosBasicosNuevos.TipoValor , TipoValor)
  
  
  this.ApplyTableDefinition()
  
}