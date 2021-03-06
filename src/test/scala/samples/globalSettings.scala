package samples


import com.huemulsolutions.bigdata.common._

object globalSettings {
   val Global: huemul_GlobalPath  = new huemul_GlobalPath()
   Global.GlobalEnvironments = "production, experimental"
   
   Global.CONTROL_Setting.append(new huemul_KeyValuePath("production","jdbc:postgresql://35.188.65.185:5432/postgres?user=postgres&password=control-postgres&currentSchema=public"))
   Global.CONTROL_Setting.append(new huemul_KeyValuePath("experimental","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))
 
   Global.IMPALA_Setting.append(new huemul_KeyValuePath("production","jdbc:postgresql://35.188.65.185:5432/postgres?user=postgres&password=control-postgres&currentSchema=public"))
   Global.IMPALA_Setting.append(new huemul_KeyValuePath("experimental","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))
   
   //TEMPORAL SETTING
   Global.TEMPORAL_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/temp/"))
   Global.TEMPORAL_Path.append(new huemul_KeyValuePath("experimental","hdfs://hdfs:///user/data/experimental/temp/"))
     
   //RAW SETTING
   Global.RAW_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/raw/"))
   Global.RAW_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))
   
   Global.RAW_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/raw/"))
   Global.RAW_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))
   
   //BACKUP SETTING
   Global.MDM_Backup_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/backup/"))
   Global.MDM_Backup_Path.append(new huemul_KeyValuePath("experimental","hdfs://hdfs:///user/data/experimental/backup/"))
   
   
   //MASTER SETTING
   Global.MASTER_DataBase.append(new huemul_KeyValuePath("production","production_master"))   
   Global.MASTER_DataBase.append(new huemul_KeyValuePath("experimental","experimental_master"))

   Global.MASTER_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/master/"))
   Global.MASTER_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/master/"))
   
   Global.MASTER_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/master/"))
   Global.MASTER_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/master/"))

   //DIM SETTING
   Global.DIM_DataBase.append(new huemul_KeyValuePath("production","production_dim"))   
   Global.DIM_DataBase.append(new huemul_KeyValuePath("experimental","experimental_dim"))

   Global.DIM_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/dim/"))
   Global.DIM_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))
   
   Global.DIM_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/dim/"))
   Global.DIM_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))

   //ANALYTICS SETTING
   Global.ANALYTICS_DataBase.append(new huemul_KeyValuePath("production","production_analytics"))   
   Global.ANALYTICS_DataBase.append(new huemul_KeyValuePath("experimental","experimental_analytics"))
   
   Global.ANALYTICS_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/analytics/"))
   Global.ANALYTICS_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))
   
   Global.ANALYTICS_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/analytics/"))
   Global.ANALYTICS_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))

   //REPORTING SETTING
   Global.REPORTING_DataBase.append(new huemul_KeyValuePath("production","production_reporting"))
   Global.REPORTING_DataBase.append(new huemul_KeyValuePath("experimental","experimental_reporting"))

   Global.REPORTING_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/reporting/"))
   Global.REPORTING_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))
   
   Global.REPORTING_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/reporting/"))
   Global.REPORTING_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))

   //SANDBOX SETTING
   Global.SANDBOX_DataBase.append(new huemul_KeyValuePath("production","production_sandbox"))
   Global.SANDBOX_DataBase.append(new huemul_KeyValuePath("experimental","experimental_sandbox"))
   
   Global.SANDBOX_SmallFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   Global.SANDBOX_SmallFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))
   
   Global.SANDBOX_BigFiles_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   Global.SANDBOX_BigFiles_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))
   
   //DQ_ERROR SETTING
   Global.DQ_SaveErrorDetails = true
   Global.DQError_DataBase.append(new huemul_KeyValuePath("production","production_DQError"))
   Global.DQError_DataBase.append(new huemul_KeyValuePath("experimental","experimental_DQError"))
   
   Global.DQError_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/dqerror/"))
   Global.DQError_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/dqerror/"))

   //OLD VALUE TRACE
   Global.MDM_SaveOldValueTrace = true
   Global.MDM_OldValueTrace_DataBase.append(new huemul_KeyValuePath("production","production_mdm_oldvalue"))
   Global.MDM_OldValueTrace_DataBase.append(new huemul_KeyValuePath("experimental","experimental_mdm_oldvalue"))
   
   Global.MDM_OldValueTrace_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/mdm_oldvalue/"))
   Global.MDM_OldValueTrace_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/mdm_oldvalue/"))

   //BACKUP
   Global.MDM_Backup_Path.append(new huemul_KeyValuePath("production","hdfs:///user/data/production/backup/"))
   Global.MDM_Backup_Path.append(new huemul_KeyValuePath("experimental","hdfs:///user/data/experimental/backup/"))

}

