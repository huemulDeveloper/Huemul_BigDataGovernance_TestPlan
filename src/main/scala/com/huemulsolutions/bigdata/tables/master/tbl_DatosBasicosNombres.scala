package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType


class tbl_DatosBasicosNombres(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(TipoTabla)
  this.setStorageType(HuemulTypeStorageType.ORC)
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setDQMaxNewRecordsNum(4)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)

  //agrega versión 2.6 nombres de campos mdm
  this.setNameForMdmFhChange("xxxDtChange4")
  this.setNameForMdmFhNew("xxxDtNew4")
  this.setNameForMDM_ProcessNew("xxxProcNew4")
  this.setNameForMdmProcessChange("xxxProcChange4")
  this.setNameForMDM_hash("xxxHash4")
  this.setNameForMDM_StatusReg("xxxStatus")

  //Agrega version 1.3
  this.setNumPartitions(2)
  
  //Agrega versión 2.0
  this.setSaveBackup(true)

  this.setPkExternalCode("USER_COD_PK")
  
  val TipoValor: HuemulColumns = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
                            .setIsPK().setDqMinLen(2, "USER_COD_MINLEN").setDqMaxLen(50, "USER_COD_MAXLEN")
  //TipoValor.setIsPK ( true)
  //TipoValor.setDqMinLen ( 2)
  //TipoValor.setDqMaxLen ( 50)
  //TipoValor.setBusinessGlossary_Id("BG_001")
  
  val IntValue: HuemulColumns = new HuemulColumns(IntegerType,true,"datos integer")
                            .setMdmEnableOldValueFullTrace().setBusinessGlossary("BG_002")
  IntValue.setNullable ( )
  //IntValue.setMdmEnableOldValueFullTrace( true)
  //IntValue.setBusinessGlossary_Id("BG_002")
  //IntValue.setDqMaxDecimalValue(Decimal.apply(10))
  
  val BigIntValue: HuemulColumns = new HuemulColumns(LongType,true,"datos BigInt").setMdmEnableOldValueFullTrace()
  BigIntValue.setNullable ()
  //BigIntValue.setMdmEnableOldValueFullTrace( true)
  
  val SmallIntValue: HuemulColumns = new HuemulColumns(ShortType,true,"datos SmallInt").setMdmEnableOldValueFullTrace()
                    .setNullable ()
  //SmallIntValue.setMdmEnableOldValueFullTrace( true)
  
  val TinyIntValue: HuemulColumns = new HuemulColumns(ShortType,true,"datos TinyInt")
            .setNullable ()
  
  val DecimalValue: HuemulColumns = new HuemulColumns(DecimalType(10,4),true,"datos Decimal(10,4)")
            .setNullable ()
  
  val RealValue: HuemulColumns = new HuemulColumns(DoubleType,true,"datos Real")
            .setNullable ()
  
  val FloatValue: HuemulColumns = new HuemulColumns(FloatType,true,"datos Float")
            .setNullable ()
  
  val StringValue: HuemulColumns = new HuemulColumns(StringType,true,"datos String")
            .setNullable ()
  
  val charValue: HuemulColumns = new HuemulColumns(StringType,true,"datos Char")
            .setNullable ()
  
  val timeStampValue: HuemulColumns = new HuemulColumns(TimestampType,true,"datos TimeStamp")
            .setNullable ()
  
  
  
  
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