package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.Decimal


class tbl_DatosBasicosErrores(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: verificar que todos los tipos de datos sean interpretados de forma correcta")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  this.setStorageType(TipoTabla)
  this.setFrequency(HuemulTypeFrequency.ANY_MOMENT)
  
  //Agrega version 1.3
  this.setNumPartitions(2)

  
  val TipoValor = new HuemulColumns(StringType,true,"Nombre del tipo de valor")
  TipoValor.setIsPK (  )
  TipoValor.setDQ_MinLen ( 2,null)
  TipoValor.setDQ_MaxLen ( 50,null)
  
  //Valida MinMax String
  val Column_DQ_MinLen = new HuemulColumns(StringType,true,"Valida minimo largo de un string")
  Column_DQ_MinLen.setNullable ()
  Column_DQ_MinLen.setDQ_MinLen ( 10,null) //3 errores
  
  val Column_DQ_MaxLen = new HuemulColumns(StringType,true,"Valida máximo largo de un string")
  Column_DQ_MaxLen.setNullable ( )
  Column_DQ_MaxLen.setDQ_MaxLen ( 10,null )//2 errores
  
  //Valida MinMax Decimal
  val Column_DQ_MinDecimalValue = new HuemulColumns(DecimalType(10,4),true,"Valida minimo valor de un decimal")
  Column_DQ_MinDecimalValue.setNullable ( )
  Column_DQ_MinDecimalValue.setDQ_MinDecimalValue ( Decimal.apply(0),null)//2 errores
  
  val Column_DQ_MaxDecimalValue = new HuemulColumns(DecimalType(10,4),true,"Valida máximo valor de un decimal")
  Column_DQ_MaxDecimalValue.setNullable ( )
  Column_DQ_MaxDecimalValue.setDQ_MaxDecimalValue ( Decimal.apply("10.124"),null ) //1 errores
  
  
  //Valida MinMax DateTime
  val Column_DQ_MinDateTimeValue = new HuemulColumns(TimestampType,true,"Valida minimo valor de una fecha")
  Column_DQ_MinDateTimeValue.setNullable ()
  Column_DQ_MinDateTimeValue.setDQ_MinDateTimeValue ( "2017-05-01",null) //3 errores
  
  val Column_DQ_MaxDateTimeValue = new HuemulColumns(TimestampType,true,"Valida máximo valor de una fecha")
  Column_DQ_MaxDateTimeValue.setNullable ()
  Column_DQ_MaxDateTimeValue.setDQ_MaxDateTimeValue ( "2017-05-01",null )//2 errores
  
  val Column_NotNull = new HuemulColumns(IntegerType,true,"datos integer - Error nulo")
  //Column_NotNull.setNullable ()
  
  val Column_IsUnique = new HuemulColumns(StringType,true,"datos string - valor unico")
  Column_IsUnique.setNullable ()
  Column_IsUnique.setIsUnique ()
  
  
  val Column_OpcionalNoMapeado = new HuemulColumns(IntegerType,false,"datos integer - Opcional no mapeado")
  Column_OpcionalNoMapeado.setDefaultValues ( "10")
  
  //Valida MinMax DateTime
  val Column_NoMapeadoDefault = new HuemulColumns(TimestampType,false,"No Mapeado Default")
  Column_DQ_MinDecimalValue.setNullable ( )
  Column_DQ_MinDecimalValue.setDefaultValues ( "2017-05-01" )
  
  
  this.ApplyTableDefinition()
  
}