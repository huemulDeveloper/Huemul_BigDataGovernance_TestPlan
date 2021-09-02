package com.huemulsolutions.bigdata.raw


import com.huemulsolutions.bigdata.datalake.HuemulTypeFileType
import com.huemulsolutions.bigdata.datalake.HuemulTypeSeparator
import com.huemulsolutions.bigdata.datalake.HuemulDataLake
import com.huemulsolutions.bigdata.datalake.HuemulDataLakeSetting
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import org.apache.spark.sql.types._

class raw_LargoDinamico(huemulLib: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulDataLake(huemulLib, Control) with Serializable  {
   this.description = "Datos Básicos para pruebas de largo dinámico"
   this.groupName = "HuemulPlanPruebas"
      
   val FormatSetting = new HuemulDataLakeSetting(huemulLib)
    FormatSetting.startDate = huemulLib.setDateTime(2010,1,1,0,0,0)
    FormatSetting.endDate = huemulLib.setDateTime(2050,12,12,0,0,0)

    //Path info
    FormatSetting.globalPath = huemulLib.globalSettings.rawBigFilesPath
    FormatSetting.localPath = "planPruebas/"
    FormatSetting.fileName = "LargoDinamico_{{TipoArchivo}}.txt"
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
    FormatSetting.dataSchemaConf.colSeparatorType = HuemulTypeSeparator.POSITION  //POSITION;CHARACTER
    
    FormatSetting.dataSchemaConf.addColumns("campo1", "codigo", StringType,"",0,10)
    FormatSetting.dataSchemaConf.addColumns("campo2", "nombre", IntegerType,"",10,20)
    FormatSetting.dataSchemaConf.addColumns("campo3", "largo dinamico", LongType, "con descripción mia",20,-1)
    
    
    //log Info
    FormatSetting.logSchemaConf.colSeparatorType = HuemulTypeSeparator.CHARACTER  //POSITION;CHARACTER;NONE
    FormatSetting.logNumRowsFieldName = null
    //Fields Info for CHARACTER
    FormatSetting.logSchemaConf.colSeparator = ";"    //SET FOR CARACTER
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
      val validanumfilas = this.dataFrameHuemul.DQ_NumRowsInterval(this, 4,4)
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




object raw_LargoDinamico_test {
  def main(args : Array[String]) {
    
    //Creación API
    val huemulLib  = new HuemulBigDataGovernance(s"BigData Fabrics - ${this.getClass.getSimpleName}", args, com.yourcompany.settings.globalSettings.global)
    val Control = new HuemulControl(huemulLib, null, HuemulTypeFrequency.MONTHLY)
    /*************** PARAMETROS **********************/
     val TestPlanGroup: String = huemulLib.arguments.getValue("TestPlanGroup", null, "Debe especificar el Grupo de Planes de Prueba")
   
    //Inicializa clase RAW  
    val DF_RAW =  new raw_LargoDinamico(huemulLib, Control)
    if (!DF_RAW.open("DF_RAW", null, 2018, 12, 31, 0, 0, 0, "ini",AplicarTrim = false)) {
      println("************************************************************")
      println("**********  E  R R O R   E N   P R O C E S O   *************")
      println("************************************************************")
      val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "LeeLargoDinamico", "LeeLargoDinamico sin error", "sin error", s"con error: ${DF_RAW.error.controlErrorErrorCode}", !DF_RAW.errorIsError)
      Control.RegisterTestPlanFeature("LargoDinamico", IdTestPlan)
    } else{
      val IdTestPlan = Control.RegisterTestPlan(TestPlanGroup, "LeeLargoDinamico", "LeeLargoDinamico sin error", "sin error", s"con error: ${DF_RAW.error.controlErrorErrorCode}", !DF_RAW.errorIsError)
      Control.RegisterTestPlanFeature("LargoDinamico", IdTestPlan)
      DF_RAW.dataFrameHuemul.dataFrame.show()
      huemulLib.spark.sql("select *, length(campo1) as largo_campo1, length(campo2) as largo_campo2, length(campo3) as largo_campo3 FROM DF_RAW").show()
      
      val res = huemulLib.spark.sql("""select cast(max(case when length(campo3) = 7 and  campo3 = '0123456' then 1 else 0 end) as Int) as p_01
                                             ,cast(max(case when length(campo3) = 4 and  campo3 = '013 ' then 1 else 0 end) as Int) as p_02
                                             ,cast(max(case when length(campo3) = 3 and  campo3 = '012' then 1 else 0 end) as Int) as p_03
                                             ,cast(max(case when length(campo3) = 21 and  campo3 = '012345678901234567890' then 1 else 0 end) as Int) as p_04
                             FROM DF_RAW""").collect()
      
      val res_p01 = res(0).getAs[Int]("p_01")
      val res_p02 = res(0).getAs[Int]("p_02")
      val res_p03 = res(0).getAs[Int]("p_03")
      val res_p04 = res(0).getAs[Int]("p_04")
      
      val IdTestPlan_p01 = Control.RegisterTestPlan(TestPlanGroup, "prueba 1", "largo 7 y texto [0123456]", "cumple", s"${if (res_p01==1) "cumple" else "no cumple"}", res_p01 == 1)
      Control.RegisterTestPlanFeature("LargoDinamico", IdTestPlan_p01)
      
      val IdTestPlan_p02 = Control.RegisterTestPlan(TestPlanGroup, "prueba 2", "largo 4 y texto [013 ]", "cumple", s"${if (res_p02==1) "cumple" else "no cumple"}", res_p02 == 1)
      Control.RegisterTestPlanFeature("LargoDinamico", IdTestPlan_p02)
      
      val IdTestPlan_p03 = Control.RegisterTestPlan(TestPlanGroup, "prueba 3", "largo 3 y texto [012]", "cumple", s"${if (res_p03==1) "cumple" else "no cumple"}", res_p03 == 1)
      Control.RegisterTestPlanFeature("LargoDinamico", IdTestPlan_p03)
      
      val IdTestPlan_p04 = Control.RegisterTestPlan(TestPlanGroup, "prueba 4", "largo 21 y texto [012345678901234567890]", "cumple", s"${if (res_p04==1) "cumple" else "no cumple"}", res_p04 == 1)
      Control.RegisterTestPlanFeature("LargoDinamico", IdTestPlan_p04)
      
      
                            
    }


//    val MyName: String = this.getClass.getSimpleName
    //Cambiar los parametros:             nombre tabla hive   ,   package base , package específico
    //DF_RAW.GenerateInitialCode(MyName, "sbif_institucion_mes","bigdata.fabrics","sbif.bancos")       
    
    Control.finishProcessOk
    
    if (Control.TestPlan_CurrentIsOK(5))
      println("Proceso OK")
      
    huemulLib.close()
  }  
}