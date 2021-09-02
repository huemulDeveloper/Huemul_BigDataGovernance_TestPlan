package com.huemulsolutions.bigdata.raw

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.datalake.{HuemulTypeFileType, HuemulTypeSeparator, HuemulDataLake, HuemulDataLakeSetting}
import org.apache.spark.sql.types._

class raw_DatosParticion(huemulLib: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulDataLake(huemulLib, Control) with Serializable  {
   this.description = "datos para probar funcionalidades de Old VAlue Trace"
   this.groupName = "HuemulPlanPruebas"
      
   val dataLakeConfig: HuemulDataLakeSetting = new HuemulDataLakeSetting(huemulLib)
     .setStartDate(2010,1,1,0,0,0)
     .setEndDate(2050,12,12,0,0,0)
     //Path & names
     .setGlobalPath(huemulLib.globalSettings.rawBigFilesPath)
     .setLocalPath("planPruebas/")
     .setFileName("DatosParticion_{{YYYY}}{{MM}}{{DD}}_{{EMPRESA}}.txt")
     .setFileType(HuemulTypeFileType.TEXT_FILE)
     .setContactName("Sebastián Rodríguez")
     //Data
     .setColumnDelimiterType(HuemulTypeSeparator.CHARACTER)
     .setColumnDelimiter("\\|")
     .addColumn("periodo", "periodo", StringType,"periodo de los datos")
     .addColumn("empresa", "empresa", StringType,"Nombre de la empresa")
     .addColumn("app", "app", StringType,"Canal utilizado")
     .addColumn("producto", "producto", StringType,"nombre producto")
     .addColumn("cantidad", "cantidad", IntegerType,"Cantidad")
     .addColumn("precio", "precio", IntegerType,"Precio")
     .addColumn("idTx", "idTx", StringType,"codigo de la transacción")
     //Header
     .setLogNumRowsColumnName(null)
       .setHeaderColumnDelimiterType(HuemulTypeSeparator.CHARACTER)
       .setHeaderColumnDelimiter("\\|")
       .setHeaderColumnsString("VACIO")

  this.settingByDate.append(dataLakeConfig)


    /***
   * open(ano: Int, mes: Int) <br>
   * método que retorna una estructura con un DF de detalle, y registros de control <br>
   * ano: año de los archivos recibidos <br>
   * mes: mes de los archivos recibidos <br>
   * dia: dia de los archivos recibidos <br>
   * Retorna: true si todo está OK, false si tuvo algún problema <br>
  */
  def open(Alias: String, ControlParent: HuemulControl, ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, Empresa: String, AplicarTrim: Boolean = true): Boolean = {
    val control = new HuemulControl(huemulLib, ControlParent, HuemulTypeFrequency.MONTHLY, false)
    //Setea parámetros
    control.AddParamYear("Ano", ano)
    control.AddParamMonth("Mes", mes)
    control.AddParamInformation("Empresa", Empresa)

    
    control.newStep("Abriendo raw")
       
    try { 
      //Abre archivo RDD y devuelve esquemas para transformar a DF
      if (!this.openFile(ano, mes, dia, hora, min, seg, s"{{EMPRESA}}=$Empresa")){
        control.raiseError(s"error al abrir archivo: ${this.error.controlErrorMessage}")
      }
   
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
      val validanumfilas = this.dataFrameHuemul.DQ_NumRowsInterval(this, 1,6000)
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




