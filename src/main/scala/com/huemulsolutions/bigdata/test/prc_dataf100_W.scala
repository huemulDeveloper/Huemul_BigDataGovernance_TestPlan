package com.huemulsolutions.bigdata.test

import java.util.Calendar

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.raw.raw_dataf100
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.master.dataf100_W

/**
 *
 */
object prc_dataf100_W {

  /**
   * Main que se ejecuta cuando se llama el JAR desde spark2-submit. el código esta preparado para hacer re-procesamiento masivo
   *
   * @param args  Parámetros de invocación
   */
  def main(args : Array[String]) {
    val arguments: HuemulArgs = new HuemulArgs()
    arguments.setArgs(args)



    val line="*********************************************************************************************************"
    println(line)
    val huemulBigDataGov  = new HuemulBigDataGovernance(s"Masterizacion tabla <paises> - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.global)
    println(line)

    /*************** PARÁMETROS **********************/
    var paramAno = huemulBigDataGov.arguments.getValue("ano", null,"Debe especificar ano de proceso: ejemplo: ano=2017").toInt
    var paramMes = huemulBigDataGov.arguments.getValue("mes", null,"Debe especificar mes de proceso: ejemplo: mes=12").toInt

    println(huemulBigDataGov.spark.sparkContext.getConf.getInt("spark.executor.instances", 1))
    huemulBigDataGov.spark.sparkContext.getExecutorMemoryStatus.foreach(println)

    println(line)

     val paramDia:Int = 1

    val paramNumMeses = huemulBigDataGov.arguments.getValue("num_meses", "1").toInt

    /*************** CICLO RE-PROCESO MASIVO **********************/
    var i: Int = 1
    val Fecha = huemulBigDataGov.setDateTime(paramAno, paramMes, paramDia, 0, 0, 0)

    while (i <= paramNumMeses) {
      paramAno = huemulBigDataGov.getYear(Fecha)
      paramMes = huemulBigDataGov.getMonth(Fecha)
      println(s"Procesando Año $paramAno, Mes $paramMes ($i de $paramNumMeses)")

      //Ejecuta código
      val finControl = processMaster(huemulBigDataGov, null, paramAno, paramMes)

      if (finControl.controlError.isOK)
        i+=1
      else {
        println(s"ERROR Procesando Año $paramAno, Mes $paramMes ($i de $paramNumMeses)")
        i=paramNumMeses+1
      }

      Fecha.add(Calendar.MONTH, 1)
    }

    huemulBigDataGov.close
  }

  /**
   * Proceso principal que procesa la reglas de carga de la fuente raw_nation
   *
   * @param huemulBigDataGov  Clase inicial de la librería Huemul Big Data Governance
   * @param ControlParent     Clase que posibilita la integración del desarrollo con el gobierno de datos
   * @param paramAno         Año del archivo a procesar
   * @param paramMes         Mes del archivo a procesar
   * @return                  Retorna clases HuemulControl
   */
  def processMaster(huemulBigDataGov: HuemulBigDataGovernance, ControlParent: HuemulControl
                    , paramAno: Integer, paramMes: Integer): HuemulControl = {
    val Control = new HuemulControl(huemulBigDataGov, ControlParent,  HuemulTypeFrequency.MONTHLY)

    try {
      /*************** AGREGAR PARÁMETROS A CONTROL **********************/
      Control.AddParamYear("param_ano", paramAno)
      Control.AddParamMonth("param_mes", paramMes)

      /*************** ABRE RAW DESDE DATA LAKE **********************/
      Control.newStep("Abre DataLake")
      val dfRawDataF100 =  new raw_dataf100(huemulBigDataGov, Control)
      if (!dfRawDataF100.open("raw_dataf100", Control, paramAno, paramMes, 1, 0, 0, 0))
        Control.raiseError(s"error encontrado, abortar: ${dfRawDataF100.error.controlErrorMessage}")

      val TipoTablaParam: String = huemulBigDataGov.arguments.getValue("TipoTabla", null, "Debe especificar TipoTabla (ORC,PARQUET,HBASE,DELTA)")
      var TipoTabla: HuemulTypeStorageType = null
      if (TipoTablaParam == "orc")
        TipoTabla = HuemulTypeStorageType.ORC
      else if (TipoTablaParam == "parquet")
        TipoTabla = HuemulTypeStorageType.PARQUET
      else if (TipoTablaParam == "delta")
        TipoTabla = HuemulTypeStorageType.DELTA
      else if (TipoTablaParam == "hbase")
        TipoTabla = HuemulTypeStorageType.HBASE
      else if (TipoTablaParam == "avro")
        TipoTabla = HuemulTypeStorageType.AVRO
      /********************************************************/
      /*************** LÓGICA DE NEGOCIO **********************/
      /********************************************************/

      //instancia de clase <paises>
      Control.newStep("Asocia columnas de la tabla ")
      val tblDataF100W = new dataf100_W(huemulBigDataGov, Control, TipoTabla)
      tblDataF100W.dfFromRaw(dfRawDataF100,"dataf100_W")
      tblDataF100W.PKCOL1.setMapping("PKCOL1")
      tblDataF100W.PKCOL2.setMapping("PKCOL2")
      tblDataF100W.UNIQUECOL.setMapping("UNIQUECOL")
      tblDataF100W.VACIOCOL.setMapping("NULLCOL")
      tblDataF100W.LENGTHCOL.setMapping("LENGTHCOL")
      tblDataF100W.DECIMALCOL.setMapping("DECIMALCOL")
      tblDataF100W.DATECOL.setMapping("DATECOL")
      tblDataF100W.REGEXCOL.setMapping("REGEXCOL")
      tblDataF100W.HIERARCHYTEST01_E.setMapping("HIERARCHYTEST01")
      tblDataF100W.HIERARCHYTEST01_W.setMapping("HIERARCHYTEST01")
      tblDataF100W.HIERARCHYTEST01_WEX.setMapping("HIERARCHYTEST01")
      tblDataF100W.HIERARCHYTEST02_E.setMapping("HIERARCHYTEST02")
      tblDataF100W.HIERARCHYTEST02_W.setMapping("HIERARCHYTEST02")
      tblDataF100W.HIERARCHYTEST02_WEX.setMapping("HIERARCHYTEST02")

      tblDataF100W.setApplyDistinct(false) //deshabilitar si DF tiene datos únicos, por default está habilitado

      Control.newStep("Ejecuta Proceso carga <paises>")
      if (!tblDataF100W.executeFull("FinalSaved"))
        Control.raiseError(s"User: error al intentar masterizar los datos (${tblDataF100W.errorCode}): ${tblDataF100W.errorText}")

      Control.finishProcessOk
    } catch {
      case e: Exception =>
        Control.controlError.setError(e, this.getClass.getName, null)
        Control.finishProcessError()
    }

    Control
  }
}
