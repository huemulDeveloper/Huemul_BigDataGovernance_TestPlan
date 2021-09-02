package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.{HuemulColumns, _}
import org.apache.spark.sql.types.DataTypes._


class tbl_DatosParticionMaster(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Master)
  this.setDataBase(HuemulLib.globalSettings.masterDataBase)
  this.setDescription("Plan pruebas: Carga datos con varias particiones")
  this.setGlobalPaths(HuemulLib.globalSettings.masterBigFilesPath)
  this.setLocalPath("planPruebas/")
  this.setStorageType(TipoTabla)
  //this.setStorageType(HuemulTypeStorageType.ORC)
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  //this.setDqMaxNewRecordsNum(value = 4)
  this.setFrequency(HuemulTypeFrequency.DAILY)

  if (TipoTabla == HuemulTypeStorageType.AVRO) {
    HuemulLib.spark.sql("set spark.sql.sources.partitionColumnTypeInference.enabled=false")
  }

  //Agrega version 1.3
  //this.setNumPartitions(2)
  
  //Agrega versión 2.0
  this.setSaveBackup(true)

  this.setPkExternalCode("USER_COD_PK")
  
  val periodo: HuemulColumns = new HuemulColumns(DateType,true,"Periodo de los datos")
                            .setIsPK().setPartitionColumn(1,dropBeforeInsert = false, oneValuePerProcess = true)

  val idTx: HuemulColumns = new HuemulColumns(StringType,true,"codigo de la transacción")
    .setIsPK()


  val EmpresA: HuemulColumns = new HuemulColumns(StringType,true,"Empresa que registra ventas")
    .setPartitionColumn(2,dropBeforeInsert = true, oneValuePerProcess = false)

  val app: HuemulColumns = new HuemulColumns(StringType,true,"app que registra ventas")
    .setPartitionColumn(3,dropBeforeInsert = false, oneValuePerProcess = false)

  val producto: HuemulColumns = new HuemulColumns(StringType,true,"producto de la venta")

  val cantidad: HuemulColumns = new HuemulColumns(IntegerType,true,"Cantidad de productos vendidos")

  val precio: HuemulColumns = new HuemulColumns(IntegerType,true,"precio de la venta")


  
  this.applyTableDefinition()
  
}