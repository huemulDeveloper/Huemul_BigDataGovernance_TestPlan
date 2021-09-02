package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType}

/**
 * Clase que abre archivo RAW nation
 *
 * @param huemulBigDataGov  Clase inicial de la librería Huemul Big Data Governance
 * @param Control           Clase que posibilita la integración del desarrollo con el gobierno de datos
 */
class raw_dataf100(huemulBigDataGov: HuemulBigDataGovernance, Control: HuemulControl)
  extends HuemulDataLake(huemulBigDataGov, Control) with Serializable  {

  this.description = "Data F100"
  this.groupName = "poc"
  this.setFrequency(HuemulTypeFrequency.MONTHLY)

  //Crea variable para configuración de lectura del archivo
  val CurrentSetting = new HuemulDataLakeSetting(huemulBigDataGov)
  //configurar la fecha de vigencia de esta configuración
  CurrentSetting.startDate = huemulBigDataGov.setDateTime(2010,1,1,0,0,0)
  CurrentSetting.endDate = huemulBigDataGov.setDateTime(2050,12,12,0,0,0)

  //Configuración de rutas globales
  CurrentSetting.globalPath = huemulBigDataGov.globalSettings.rawSmallFilesPath
  //Configura ruta local, se pueden usar comodines
  CurrentSetting.localPath = "planPruebas/"
  //configura el nombre del archivo (se pueden usar comodines)
  CurrentSetting.fileName = "TestDataF100.csv"
  //especifica el tipo de archivo a leer
  CurrentSetting.fileType = HuemulTypeFileType.TEXT_FILE
  //especifica el nombre del contacto del archivo en TI
  CurrentSetting.contactName = "Christian Sattler"

  //Indica como se lee el archivo
  CurrentSetting.dataSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER  //POSITION;CHARACTER
  //separador de columnas
  CurrentSetting.dataSchemaConf.colSeparator = ","    //SET FOR CHARACTER

  CurrentSetting.dataSchemaConf.addColumns("PKCOL1", "", IntegerType, "")
  CurrentSetting.dataSchemaConf.addColumns("PKCOL2", "", IntegerType, "")
  CurrentSetting.dataSchemaConf.addColumns("UNIQUECOL", "", IntegerType, "")
  CurrentSetting.dataSchemaConf.addColumns("NULLCOL", "", IntegerType, "")
  CurrentSetting.dataSchemaConf.addColumns("LENGTHCOL", "", StringType, "")
  CurrentSetting.dataSchemaConf.addColumns("DECIMALCOL", "", IntegerType, "")
  CurrentSetting.dataSchemaConf.addColumns("DATECOL", "", DateType, "")
  CurrentSetting.dataSchemaConf.addColumns("REGEXCOL", "", StringType, "")
  CurrentSetting.dataSchemaConf.addColumns("HIERARCHYTEST01", "", StringType, "")
  CurrentSetting.dataSchemaConf.addColumns("HIERARCHYTEST02", "", StringType, "")

  //Configurar de lectura de información de log (en caso de tener, si no tiene se configura HuemulTypeSeparator.NONE)
  CurrentSetting.logSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER
  CurrentSetting.logSchemaConf.colSeparator=","
  CurrentSetting.logSchemaConf.setHeaderColumnsString("HEADER")
  CurrentSetting.logNumRowsFieldName = null


  this.settingByDate.append(CurrentSetting)

  /**
   * Método que retorna una estructura con un DF de detalle, y registros de control
   *
   * @param Alias         Alias del dataFrame SQL
   * @param ControlParent ControlParent
   * @param year          Año del archivo
   * @param month         Mes del archivo
   * @param day           Dia del archivo
   * @param hour          Hora del archivo
   * @param min           Minuto del archivo
   * @param seg           Segundo Hora del archivo
   * @return              Boolean: True si la apertura del archivo fue exitosa
   */
  def open(Alias: String, ControlParent: HuemulControl
           , year: Integer, month: Integer, day: Integer, hour: Integer, min: Integer, seg: Integer): Boolean = {
    //Crea registro de control de procesos
    val control = new HuemulControl(huemulBigDataGov, ControlParent, HuemulTypeFrequency.ANY_MOMENT)

    //Guarda los parámetros importantes en el control de procesos
    control.AddParamYear("Ano", year)
    control.AddParamMonth("Mes", month)

    try {
      //newStep va registrando los pasos de este proceso, también sirve como documentación del mismo.
      control.newStep("Abre archivo RDD y devuelve esquemas para transformar a DF")
      if (!this.openFile(year, month, day, hour, min, seg, null)){
        //Control también entrega mecanismos de envío de excepciones
        control.raiseError(s"error al abrir archivo: ${this.error.controlErrorMessage}")
      }

      //Si el archivo no tiene cabecera, comentar la línea de .filter
      control.newStep("Aplicando Filtro")
      val rowRDD = this.dataRdd
        //filtro para dejar fuera la primera fila
        .filter { x => x != this.log.dataFirstRow  }
        .map { x => this.convertSchema(x) }

      //Crea dataFrame en Data.DataDF
      control.newStep("Transformando datos a dataFrame")
      this.dfFromRaw(rowRDD, Alias)

      //************************
      //**** VALIDACIÓN DQ *****
      //************************
      control.newStep("Valida que cantidad de registros esté entre 10 y 500")
      //validación cantidad de filas
      val validaNumFilas = this.dataFrameHuemul.DQ_NumRowsInterval(this, 1, 100)
      if (validaNumFilas.isError) control.raiseError(s"user: Numero de Filas fuera del rango. ${validaNumFilas.description}")

      control.finishProcessOk
    } catch {
      case e: Exception =>
        control.controlError.setError(e, this.getClass.getName, null)
        control.finishProcessError()
    }
    control.controlError.isOK
  }
}

//---------------------------------------------------------------------------------------------------------------------
/**
 * Este objeto se utiliza solo para probar la lectura del archivo RAW.
 * La clase que está definida más abajo se utiliza para la lectura.
 */
object raw_dataf100_test {

  /**
   * Proceso main que permite  probar la configuración del archivo RAW
   *
   * @param args   Parámetros de invocación
   */
  def main(args : Array[String]) {


    //Creación API
    val huemulBigDataGov  = new HuemulBigDataGovernance(s"Testing DataLake - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.global)

    //Creación del objeto control, por default no permite ejecuciones en paralelo del mismo objeto (corre en modo SINGLETON)
    val Control = new HuemulControl(huemulBigDataGov, null, HuemulTypeFrequency.MONTHLY )

    /*************** PARÁMETROS **********************/
    val param_year = huemulBigDataGov.arguments.getValue("year", null
      , "Debe especificar el parámetro año, ej: year=2017").toInt
    val param_month = huemulBigDataGov.arguments.getValue("month", null
      , "Debe especificar el parámetro mes, ej: month=12").toInt

    //Inicializa clase RAW
    val DF_RAW =  new raw_dataf100(huemulBigDataGov, Control)
    if (!DF_RAW.open("DF_RAW", null, param_year, param_month, 0, 0, 0, 0)) {
      huemulBigDataGov.log_info.error("************************************************************")
      huemulBigDataGov.log_info.error("**********  E  R R O R   E N   P R O C E S O   *************")
      huemulBigDataGov.log_info.error("************************************************************")

    } else
      DF_RAW.dataFrameHuemul.dataFrame.show()

    Control.finishProcessOk
    huemulBigDataGov.close()

  }
}
