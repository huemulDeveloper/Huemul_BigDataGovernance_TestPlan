package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.datalake.HuemulTypeFileType
import com.huemulsolutions.bigdata.datalake.HuemulTypeSeparator
import com.huemulsolutions.bigdata.datalake.HuemulDataLake
import com.huemulsolutions.bigdata.datalake.HuemulDataLakeSetting
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import org.apache.spark.sql.types._


class raw_DatosOldValue(huemulLib: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulDataLake(huemulLib, Control) with Serializable  {
   this.description = "datos para probar funcionalidades de Old VAlue Trace"
   this.groupName = "HuemulPlanPruebas"
      
   val FormatSetting = new HuemulDataLakeSetting(huemulLib)
    FormatSetting.startDate = huemulLib.setDateTime(2010,1,1,0,0,0)
    FormatSetting.endDate = huemulLib.setDateTime(2050,12,12,0,0,0)

    //Path info
    FormatSetting.globalPath = huemulLib.globalSettings.rawBigFilesPath
    FormatSetting.localPath = "planPruebas/"
    FormatSetting.fileName = "OldValueTrace_{{TipoArchivo}}.txt"
    FormatSetting.fileType = HuemulTypeFileType.TEXT_FILE
    FormatSetting.contactName = "Sebastián Rodríguez"
    
    //Columns Info CHARACTER
    
    //PLAN EJECUCION 1:
    /*
    FormatSetting.dataSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.dataSchemaConf.colSeparator = "\\|"    //SET FOR CARACTER
    FormatSetting.dataSchemaConf.setHeaderColumnsString("TipoValor;IntValue;BigIntValue;SmallIntValue;TinyIntValue;DecimalValue;RealValue;FloatValue;StringValue;charValue;timeStampValue") //siempre con ;
    *  
    */
    
    //PLAN EJECUCION 2:
    /*
    FormatSetting.dataSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.dataSchemaConf.colSeparator = "\\|"    //SET FOR CARACTER
    
    FormatSetting.dataSchemaConf.addColumns("TipoValor", "TipoValor_ti", StringType)
    FormatSetting.dataSchemaConf.addColumns("IntValue", "IntValue_ti", IntegerType)
    FormatSetting.dataSchemaConf.addColumns("BigIntValue", "BigIntValue_ti", LongType, "con descripción mia")
    FormatSetting.dataSchemaConf.addColumns("SmallIntValue", "SmallIntValue_ti", ShortType)
    FormatSetting.dataSchemaConf.addColumns("TinyIntValue", "TinyIntValue_ti", ShortType)
    FormatSetting.dataSchemaConf.addColumns("DecimalValue", "DecimalValue_ti", DecimalType(10,4))
    FormatSetting.dataSchemaConf.addColumns("RealValue", "RealValue_ti", DoubleType)
    FormatSetting.dataSchemaConf.addColumns("FloatValue", "FloatValue_ti", FloatType)
    FormatSetting.dataSchemaConf.addColumns("StringValue", "StringValue_ti", StringType)
    FormatSetting.dataSchemaConf.addColumns("charValue", "charValue_ti", StringType)
    FormatSetting.dataSchemaConf.addColumns("timeStampValue", "timeStampValue_ti", TimestampType)
    * 
    */
    
    //PLAN EJECUCION 3:
    FormatSetting.dataSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER  //POSITION;CHARACTER
    FormatSetting.dataSchemaConf.colSeparator = "\\|"
    
    FormatSetting.dataSchemaConf.addColumns("codigo", "codigo", StringType,"")
    FormatSetting.dataSchemaConf.addColumns("descripcion", "descripcion_ti", IntegerType,"")
    FormatSetting.dataSchemaConf.addColumns("fecha", "fecha_ti", TimestampType,"")
    FormatSetting.dataSchemaConf.addColumns("monto", "monto_ti", IntegerType,"")
    
    
    //log Info
    FormatSetting.logSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER  //POSITION;CHARACTER;NONE
    FormatSetting.logNumRowsFieldName = null
    //Fields Info for CHARACTER
    FormatSetting.logSchemaConf.colSeparator = "|"    //SET FOR CARACTER
    FormatSetting.logSchemaConf.setHeaderColumnsString("VACIO") //Fielda;Fieldb;fieldc
    
    this.settingByDate.append(FormatSetting)
  
    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: HuemulControl, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, TipoArchivo: String, AplicarTrim: Boolean = true): Boolean = {
    val control = new HuemulControl(huemulLib, ControlParent, HuemulTypeFrequency.MONTHLY, false)
    //Setea parámetros
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
    
    control.newStep("Abriendo raw")
       
    try { 
      //Abre archivo RDD y devuelve esquemas para transformar a DF
      if (!this.openFile(ano, mes, dia, hora, min, seg, s"{{TipoArchivo}}=$TipoArchivo")){
        control.raiseError(s"error al abrir archivo: ${this.error.controlErrorMessage}")
      }
      
      //import huemulLib.spark.implicits._
   
      control.newStep("Aplicando Filtro")
      /**/    //Agregar filtros o cambiar forma de leer archivo en este lugar
      this.applyTrim = AplicarTrim
     // this.allColumnsAsString(false)
      val rowRDD = this.dataRdd
            .filter { x => x != this.log.dataFirstRow  }
            .map(  x => {this.convertSchema(x)} )
        
            
      control.newStep("Transformando a dataframe")
      //Crea dataFrame en Data.DataDF
      this.dfFromRaw(rowRDD, Alias)
        
      //****VALIDACION DQ*****
      //**********************
      
      control.newStep("Validando cantidad de filas")
      //validacion cantidad de filas
      val validanumfilas = this.dataFrameHuemul.DQ_NumRowsInterval(this, 6,6)
      if (validanumfilas.isError) control.raiseError(s"user: Numero de Filas fuera del rango. ${validanumfilas.description}")
                        
      control.finishProcessOk
    } catch {
      case e: Exception =>
        control.controlError.setError(e, this.getClass.getName, this, null)
        control.finishProcessError()
    }         
    control.controlError.isOK
  }
}




object raw_DatosOldValue {
  def main(args : Array[String]) {
    
    //Creación API
    val huemulLib  = new HuemulBigDataGovernance(s"BigData Fabrics - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib, null, HuemulTypeFrequency.MONTHLY)
    /*************** PARAMETROS **********************/
    
    //Inicializa clase RAW  
    val DF_RAW =  new raw_DatosOldValue(huemulLib, Control)
    if (!DF_RAW.open("DF_RAW", null, 2018, 12, 31, 0, 0, 0, "ini")) {
      println("************************************************************")
      println("**********  E  R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
    } else
      DF_RAW.dataFrameHuemul.dataFrame.show()
      
    
    this.getClass.getSimpleName
    //Cambiar los parametros:             nombre tabla hive   ,   package base , package específico
    //DF_RAW.GenerateInitialCode(MyName, "sbif_institucion_mes","bigdata.fabrics","sbif.bancos")       
    
    Control.finishProcessOk
  }  
}