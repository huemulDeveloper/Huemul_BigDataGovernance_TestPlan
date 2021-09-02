package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._


class tbl_DatosBasicosInsert_exclude(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta (carga 1 vez, luego solo inserta datos)")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
  this.setStorageType(TipoTabla)
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
    //Agrega version 1.3
  this.setNumPartitions(2)
  
  //Agrega versión 2.0
  this.setSaveBackup(true)

  
  val TipoValor = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK ( )
  TipoValor.setDqMinLen ( 2)
  TipoValor.setDqMaxLen ( 50)
  
  
  val IntValue = new HuemulColumns(IntegerType,true,"datos integer")
  IntValue.setNullable ( )
  IntValue.setMdmEnableDTLog ( )
  IntValue.setMdmEnableOldValue ( )
  IntValue.setMdmEnableProcessLog ( )
  
  val BigIntValue = new HuemulColumns(LongType,true,"datos BigInt")
  BigIntValue.setNullable ( )
  BigIntValue.setMdmEnableDTLog ( )
  BigIntValue.setMdmEnableOldValue ( )
  BigIntValue.setMdmEnableProcessLog ( )
  
  val SmallIntValue = new HuemulColumns(ShortType,true,"datos SmallInt")
  SmallIntValue.setNullable ( )
  SmallIntValue.setMdmEnableDTLog ( )
  SmallIntValue.setMdmEnableOldValue ( )
  SmallIntValue.setMdmEnableProcessLog ( )
  
  val TinyIntValue = new HuemulColumns(ShortType,true,"datos TinyInt")
  TinyIntValue.setNullable ( )
  TinyIntValue.setMdmEnableDTLog ( )
  TinyIntValue.setMdmEnableOldValue ( )
  TinyIntValue.setMdmEnableProcessLog ( )
  
  val DecimalValue = new HuemulColumns(DecimalType(10,4),true,"datos Decimal(10,4)")
  DecimalValue.setNullable ( )
  DecimalValue.setMdmEnableDTLog ( )
  DecimalValue.setMdmEnableOldValue ( )
  DecimalValue.setMdmEnableProcessLog ( )
  
  val RealValue = new HuemulColumns(DoubleType,true,"datos Real")
  RealValue.setNullable ( )
  RealValue.setMdmEnableDTLog ( )
  RealValue.setMdmEnableOldValue ( )
  RealValue.setMdmEnableProcessLog ( )
  
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
  
  //Regla para probar exclusión de registro al fallar un warning
  val DQ_warning_exclude: HuemulDataQuality = new HuemulDataQuality(TipoValor ,"Exclusión de valor Cero-Vacio", "tipoValor not in ('Cero-Vacio')",1).setNotification(HuemulTypeDqNotification.WARNING_EXCLUDE).setQueryLevel(HuemulTypeDqQueryLevel.Row)
  val DQ_warning_solo: HuemulDataQuality = new HuemulDataQuality(TipoValor ,"Solo warning cuando aparezca registro Cero-Vacio", "tipoValor <> 'Negativo_Maximo'",2).setNotification(HuemulTypeDqNotification.WARNING).setQueryLevel(HuemulTypeDqQueryLevel.Row)
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicosNuevos = new tbl_DatosBasicosNuevos(HuemulLib,Control, TipoTabla)
  val fk_tbl_DatosBasicos: HuemulTableRelationship = new HuemulTableRelationship(itbl_DatosBasicosNuevos, false).setNotification(HuemulTypeDqNotification.WARNING_EXCLUDE).broadcastJoin(true)
  fk_tbl_DatosBasicos.addRelationship(itbl_DatosBasicosNuevos.TipoValor , TipoValor)
  
  
  this.applyTableDefinition()
  
}