package samples

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.tables.master._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.tables._

@Test
class AppTest {
    val args: Array[String] = new Array[String](1)
    args(0) = "Environment=production,RegisterInControl=false,TestPlanMode=true"
      
    val huemulLib = new HuemulBigDataGovernance("Pruebas Inicialización de Clases",args,globalSettings.global)
    val Control = new HuemulControl(huemulLib,null, HuemulTypeFrequency.MONTHLY)
      
    @Test
    def pruebita(): Unit = assertTrue(genera()
      
    )
    
      
    @Test
    def TEST_tbl_DatosBasicos_Mes(): Unit = assertTrue(TESTp_tbl_DatosBasicos_mes())
    
    @Test
    def TEST_tbl_DatosBasicos(): Unit = assertTrue(TESTp_tbl_DatosBasicos())
    
    @Test
    def TEST_tbl_DatosBasicosAVRO(): Unit = assertTrue(TESTp_tbl_DatosBasicosAVRO())
    
//    @Test
//    def TEST_tbl_DatosBasicosInsesrt() = assertTrue(TESTp_tbl_DatosBasicosInsert())
    
//    @Test
//    def TEST_tbl_DatosBasicosErrores() = assertTrue(TESTp_tbl_DatosBasicosErrores())
    
    @Test
    def TEST_tbl_DatosBasicosUpdate(): Unit = assertTrue(TESTp_tbl_DatosBasicosUpdate())
    
    def genera(): Boolean = {
      //val a = new raw_DatosBasicos(huemulLib, Control)
      
      //a.GenerateInitialCode("com.yourapplication", "objectName", "tbl_algo", "test/", huemulType_Tables.Reference, huemulType_Frequency.MONTHLY)
       true
    }
    
    /**Revisión de clase tbl_DatosBasicos
     * 
     */
    def TESTp_tbl_DatosBasicos(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicos(huemulLib,Control)
        if (Master.errorIsError) {
          println(s"Codigo: ${Master.errorCode}, Descripción: ${Master.errorText}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
       SinError
    }
    
    /**Revisión de clase tbl_DatosBasicos
     * 
     */
    def TESTp_tbl_DatosBasicos_mes(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicos_mes(huemulLib,Control,HuemulTypeStorageType.HBASE)
        if (Master.errorIsError) {
          println(s"Codigo: ${Master.errorCode}, Descripción: ${Master.errorText}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
       SinError
    }
    
    /**Revisión de clase tbl_DatosBasicosAVRO
     * 
     */
    def TESTp_tbl_DatosBasicosAVRO(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosAVRO(huemulLib,Control,HuemulTypeStorageType.ORC)
        if (Master.errorIsError) {
          println(s"Codigo: ${Master.errorCode}, Descripción: ${Master.errorText}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
       SinError
    }

    /**Revisión de clase tbl_DatosBasicosInsert
     * 
     */
    def TESTp_tbl_DatosBasicosInsert(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosInsert(huemulLib,Control,HuemulTypeStorageType.HBASE)
        if (Master.errorIsError) {
          println(s"Codigo: ${Master.errorCode}, Descripción: ${Master.errorText}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
       SinError
    }

    /**Revisión de clase tbl_DatosBasicosErrores
     * 
     */
    def TESTp_tbl_DatosBasicosErrores(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosErrores(huemulLib,Control,HuemulTypeStorageType.HBASE)
        if (Master.errorIsError) {
          println(s"Codigo: ${Master.errorCode}, Descripción: ${Master.errorText}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
       SinError
    }

    /**Revisión de clase tbl_DatosBasicosUpdate
     * 
     */
    def TESTp_tbl_DatosBasicosUpdate(): Boolean = {
      var SinError = true
      
      try {
        val Master = new tbl_DatosBasicosUpdate(huemulLib,Control,HuemulTypeStorageType.PARQUET)
        if (Master.errorIsError) {
          println(s"Codigo: ${Master.errorCode}, Descripción: ${Master.errorText}")
          SinError = false
        }
      } catch {
        case e: Exception => 
          SinError = false
          println(e)
      }
        
      
       SinError
    }



}

