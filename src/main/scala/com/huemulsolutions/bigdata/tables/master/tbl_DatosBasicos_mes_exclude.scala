package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType



class tbl_DatosBasicos_mes_exclude(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Transaction)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
  if (TipoTabla == HuemulTypeStorageType.HBASE)
    this.setStorageType(HuemulTypeStorageType.PARQUET)
  else 
    this.setStorageType(TipoTabla)
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setDQMaxNewRecordsNum(4)
  //this.setPartitionField("periodo_mes")
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)
  
  val periodo_mes = new HuemulColumns(StringType,true,"periodo")
  periodo_mes.setIsPK ( )
  periodo_mes.setPartitionColumn(1)

  
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
  
  //Regla para probar exclusión de registro al fallar un warning
  val DQ_warning_exclude: HuemulDataQuality = new HuemulDataQuality(TipoValor ,"Exclusión de valor Cero-Vacio", "tipoValor not in ('Cero-Vacio')",1).setNotification(HuemulTypeDqNotification.WARNING_EXCLUDE).setQueryLevel(HuemulTypeDqQueryLevel.Row)
  val DQ_warning_solo: HuemulDataQuality = new HuemulDataQuality(TipoValor ,"Solo warning cuando aparezca registro Cero-Vacio", "tipoValor <> 'Negativo_Maximo'",2).setNotification(HuemulTypeDqNotification.WARNING).setQueryLevel(HuemulTypeDqQueryLevel.Row)
  
  
  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  val itbl_DatosBasicos = new tbl_DatosBasicos(HuemulLib,Control)
  val fk_tbl_DatosBasicos: HuemulTableRelationship = new HuemulTableRelationship(itbl_DatosBasicos, false).setNotification(HuemulTypeDqNotification.WARNING_EXCLUDE).broadcastJoin(true)
  fk_tbl_DatosBasicos.addRelationship(itbl_DatosBasicos.TipoValor , TipoValor)
  
  
  this.applyTableDefinition()
  
}