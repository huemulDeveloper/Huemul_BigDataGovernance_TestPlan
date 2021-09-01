package com.huemulsolutions.bigdata.tables.master
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.control.{HuemulTypeFrequency, HuemulControl}
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType
import org.apache.spark.sql.types.{DateType, Decimal, IntegerType, StringType}

class dataf100_WEX_W(huemulBigDataGov: HuemulBigDataGovernance, Control: HuemulControl, TipoTabla: HuemulTypeStorageType)
  extends HuemulTable(huemulBigDataGov, Control) with Serializable {

  val notifLevel: HuemulTypeDQNotification.Value = HuemulTypeDQNotification.WARNING

  /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
  //Tipo de tabla, Master y Reference son catálogos sin particiones de periodo
  this.setTableType(HuemulTypeTables.Reference)
  //Base de Datos en HIVE donde sera creada la tabla
  this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
  //Tipo de archivo que sera almacenado en HDFS
  //if (TipoTabla == HuemulTypeStorageType.HBASE)
  //  this.setStorageType(HuemulTypeStorageType.PARQUET)
  //else
    this.setStorageType(TipoTabla)
  //Ruta en HDFS donde se guardara el archivo PARQUET
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
  //Ruta en HDFS especifica para esta tabla (GlobalPaths / localPath)
  this.setLocalPath("planPruebas/")
  //columna de partición
  //this.setPartitionField("periodo_mes")
  //Frecuencia de actualización de los datos
  this.setFrequency(HuemulTypeFrequency.MONTHLY)
  //permite asignar un código de error personalizado al fallar la PK
  this.setPK_externalCode("ERROR_PK")
  this.setPkNotification(notifLevel)


  /**********   O P T I M I Z A C I O N  ****************************************/
  //nuevo desde version 2.0
  //Indica la cantidad de particiones al guardar un archivo, para archivos pequeños (menor al bloque de HDFS) se
  //recomienda el valor 1, mientras mayor la tabla la cantidad de particiones debe ser mayor para aprovechar el paralelismo
  this.setNumPartitions(1)

  /**********   C O N T R O L   D E   C A M B I O S   Y   B A C K U P   ****************************************/
  //Permite guardar los errores y warnings en la aplicación de reglas de DQ, valor por default es true
  this.setSaveDQResult(true)
  //Permite guardar backup de tablas maestras
  this.setSaveBackup(false)  //default value = false

  /**********   S E T E O   I N F O R M A T I V O   ****************************************/
  //Nombre del contacto de TI
  this.setDescription("Tabla referencia de datso prueba F100")
  //Nombre del contacto de negocio
  this.setBusiness_ResponsibleName("Juan Nadie <jnadie@fit.com>")
  //Nombre del contacto de TI
  this.setIT_ResponsibleName("Christian Sattler <csattler@fit.com>")

  /**********   D A T A   Q U A L I T Y   ****************************************/
  //DataQuality: maximo numero de filas o porcentaje permitido, dejar comentado o null en caso de no aplicar
  //this.setDQ_MaxNewRecords_Num(null)  //ej: 1000 para permitir maximo 1.000 registros nuevos cada vez que se intenta insertar
  //this.setDQ_MaxNewRecords_Perc(null) //ej: 0.2 para limitar al 20% de filas nuevas

  /**********   S E G U R I D A D   ****************************************/
  //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeFull_addAccess("process_entidad_mes", "com.yourcompany.yourapplication")
  //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyInsert_addAccess("[[MyclassName]]", "[[my.package.path]]")
  //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar
  //this.WhoCanRun_executeOnlyUpdate_addAccess("[[MyclassName]]", "[[my.package.path]]")


  /**********   C O L U M N A S   ****************************************/



  //Columna de periodo
  val PKCOL1: HuemulColumns = new HuemulColumns (IntegerType, true,"PKCOL1")
    .setIsPK()

  val PKCOL2: HuemulColumns = new HuemulColumns (IntegerType, true,"PKCOL2")
    .setIsPK()

  val UNIQUECOL: HuemulColumns = new HuemulColumns (IntegerType, true,"UNIQUECOL")
    .setIsUnique("ERROR_UNIQUE")
    .setIsUnique_Notification(notifLevel)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val VACIOCOL: HuemulColumns = new HuemulColumns (IntegerType, true,"NULLCOL")
    .setDQ_Nullable_Notification(notifLevel)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val LENGTHCOL: HuemulColumns = new HuemulColumns (StringType, true,"LENGTHCOL")
    .setDQ_MinLen(3,"ERROR_LEN_MIN")
    .setDQ_MaxLen(7,"ERROR_LEN_MAX")
    .setDQ_MinLen_Notification(notifLevel)
    .setDQ_MaxLen_Notification(HuemulTypeDQNotification.WARNING_EXCLUDE)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val DECIMALCOL: HuemulColumns = new HuemulColumns (IntegerType, true,"DECIMALCOL")
    .setDQ_MinDecimalValue(Decimal(1234),"ERROR_DECIMAL_MIN")
    .setDQ_MaxDecimalValue(Decimal(1234567),"ERROR_DECIMAL_MAX")
    .setDQ_MinDecimalValue_Notification(notifLevel)
    .setDQ_MaxDecimalValue_Notification(HuemulTypeDQNotification.WARNING_EXCLUDE)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val DATECOL: HuemulColumns = new HuemulColumns (DateType, true, "DATECOL")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MinDateTimeValue("2020-06-02","ERROR_DATE_MIN")
    .setDQ_MaxDateTimeValue("2020-06-04","ERROR_DATE_MAX")
    .setDQ_MinDateTimeValue_Notification(notifLevel)
    .setDQ_MaxDateTimeValue_Notification(HuemulTypeDQNotification.WARNING_EXCLUDE)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val REGEXCOL: HuemulColumns = new HuemulColumns (StringType, true, "REGEXCOL")
    .setDQ_RegExpression("^A*B*$")
    .setDQ_RegExpression_Notification(notifLevel)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)


  val HIERARCHYTEST01_E: HuemulColumns = new HuemulColumns (IntegerType, true, "HIERARCHYTEST01")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MaxLen(8,"ERROR_LEN_MAX")
    .setDQ_MaxLen_Notification(notifLevel)
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val HIERARCHYTEST01_W: HuemulColumns = new HuemulColumns (IntegerType, true, "HIERARCHYTEST02")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MaxLen(8,"ERROR_LEN_MAX")
    .setDQ_MaxLen_Notification(notifLevel)
    .setDQ_Notification(HuemulTypeDQNotification.WARNING)

  val HIERARCHYTEST01_WEX: HuemulColumns = new HuemulColumns (IntegerType, true, "HIERARCHYTEST02")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MaxLen(8,"ERROR_LEN_MAX")
    .setDQ_MaxLen_Notification(notifLevel)
    .setDQ_Notification(HuemulTypeDQNotification.WARNING_EXCLUDE)

  val HIERARCHYTEST02_E: HuemulColumns = new HuemulColumns (IntegerType, true, "HIERARCHYTEST01")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MaxLen(8,"ERROR_LEN_MAX")
    .setDQ_Notification(HuemulTypeDQNotification.ERROR)

  val HIERARCHYTEST02_W: HuemulColumns = new HuemulColumns (IntegerType, true, "HIERARCHYTEST02")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MaxLen(8,"ERROR_LEN_MAX")
    .setDQ_Notification(HuemulTypeDQNotification.WARNING)

  val HIERARCHYTEST02_WEX: HuemulColumns = new HuemulColumns (IntegerType, true, "HIERARCHYTEST02")
    .securityLevel(HuemulTypeSecurityLevel.Public)
    .setDQ_MaxLen(9,"ERROR_LEN_MAX")
    .setDQ_Notification(HuemulTypeDQNotification.WARNING_EXCLUDE)

  //**********Atributos adicionales de DataQuality
  /*
            .setIsPK()         //por default los campos no son PK
            .setIsUnique("COD_ERROR") //por default los campos pueden repetir sus valores
            .setNullable() //por default los campos no permiten nulos
            .setDQ_MinDecimalValue(Decimal.apply(0),"COD_ERROR")
            .setDQ_MaxDecimalValue(Decimal.apply(200.0),"COD_ERROR")
            .setDQ_MinDateTimeValue("2018-01-01","COD_ERROR")
            .setDQ_MaxDateTimeValue("2018-12-31","COD_ERROR")
            .setDQ_MinLen(5,"COD_ERROR")
            .setDQ_MaxLen(100,"COD_ERROR")
            .setDQ_RegExpresion("","COD_ERROR")                          //desde versión 2.0
  */
  //**********Atributos adicionales para control de cambios en los datos maestros
  /*
   					.setMDM_EnableDTLog()
  					.setMDM_EnableOldValue()
  					.setMDM_EnableProcessLog()
  					.setMDM_EnableOldValue_FullTrace()     //desde 2.0: guarda cada cambio de la tabla maestra en tabla de trace
  */
  //**********Otros atributos de clasificación
  /*
  					.encryptedType("tipo")
  					.setARCO_Data()
  					.securityLevel(HuemulTypeSecurityLevel.Public)
  					.setBusinessGlossary("CODIGO")           //desde 2.0: enlaza id de glosario de términos con campos de la tabla
  */
  //**********Otros atributos
  /*
            .setDefaultValues("'string'") // "10" // "'2018-01-01'"
  				  .encryptedType("tipo")
  */

  //**********Ejemplo para aplicar DataQuality de Integridad Referencial
  //val i[[tbl_PK]] = new [[tbl_PK]](huemulBigDataGov,Control)
  //val fk_[[tbl_PK]] = new HuemulTableRelationship(i[[tbl_PK]], false)
  //fk_[[tbl_PK]].AddRelationship(i[[tbl_PK]].[[PK_Id]], [[LocalField]_Id)

  //**********Ejemplo para agregar reglas de DataQuality Avanzadas  -->ColumnXX puede ser null si la validacion es a nivel de tabla
  //**************Parametros
  //********************  ColumnXXColumna a la cual se aplica la validacion, si es a nivel de tabla poner null
  //********************  Descripcion de la validacion, ejemplo: "Consistencia: Campo1 debe ser mayor que campo 2"
  //********************  Formula SQL En Positivo, ejemplo1: campo1 > campo2  ;ejemplo2: sum(campo1) > sum(campo2)
  //********************  CodigoError: Puedes especificar un codigo para la captura posterior de errores, es un numero entre 1 y 999
  //********************  QueryLevel es opcional, por default es "row" y se aplica al ejemplo1 de la formula, para el ejmplo2 se debe indicar "Aggregate"
  //********************  Notification es opcional, por default es "error", y ante la aparicion del error el programa falla, si lo cambias a "warning" y la validacion falla, el programa sigue y solo sera notificado
  //********************  SaveErrorDetails es opcional, por default es "true", permite almacenar el detalle del error o warning en una tabla específica, debe estar habilitada la opción DQ_SaveErrorDetails en GlobalSettings
  //********************  DQ_ExternalCode es opcional, por default es "null", permite asociar un Id externo de DQ
  //val DQ_NombreRegla: HuemulDataQuality = new HuemulDataQuality(ColumnXX,"Descripcion de la validacion", "Campo_1 > Campo_2",1)
  //**************Adicionalmente, puedes agregar "tolerancia" a la validacion, es decir, puedes especiicar
  //************** numFilas = 10 para permitir 10 errores (al 11 se cae)
  //************** porcentaje = 0.2 para permitir una tolerancia del 20% de errores
  //************** ambos parametros son independientes (condicion o), cualquiera de las dos tolerancias que no se cumpla se gatilla el error o warning
  //DQ_NombreRegla.setTolerance(numfilas, porcentaje)
  //DQ_NombreRegla.setDQ_ExternalCode("Cod_001")



  this.ApplyTableDefinition()
}