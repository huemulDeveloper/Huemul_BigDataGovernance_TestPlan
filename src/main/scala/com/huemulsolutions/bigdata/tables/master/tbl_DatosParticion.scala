package com.huemulsolutions.bigdata.tables.master

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulColumns
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
import org.apache.spark.sql.types.DataTypes._



class tbl_DatosParticion(HuemulLib: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType) extends HuemulTable(HuemulLib,Control) with Serializable {
  this.setTableType(HuemulTypeTables.Transaction)
  this.setDataBase(HuemulLib.GlobalSettings.MASTER_DataBase)
  this.setDescription("Plan pruebas: Carga datos con varias particiones")
  this.setGlobalPaths(HuemulLib.GlobalSettings.MASTER_BigFiles_Path)
  this.setLocalPath("planPruebas/")
  this.setStorageType(TipoTabla)
  //this.setStorageType(HuemulTypeStorageType.ORC)
  //this.setStorageType(HuemulTypeStorageType.PARQUET)
  //this.setDQ_MaxNewRecords_Num(value = 4)
  this.setFrequency(HuemulTypeFrequency.DAILY)
  
  //Agrega version 1.3
  //this.setNumPartitions(2)
  
  //Agrega versión 2.0
  //this.setSaveBackup(true)

  this.setPK_externalCode("USER_COD_PK")
  
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


  
  this.ApplyTableDefinition()
  
}