package samples


import com.huemulsolutions.bigdata.common._

object globalSettings {
   val global: HuemulGlobalPath  = new HuemulGlobalPath()
   global.globalEnvironments = "production, experimental"

   global.controlSetting.append(new HuemulKeyValuePath("production","jdbc:postgresql://35.188.65.185:5432/postgres?user=postgres&password=control-postgres&currentSchema=public"))
   global.controlSetting.append(new HuemulKeyValuePath("experimental","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))

   global.impalaSetting.append(new HuemulKeyValuePath("production","jdbc:postgresql://35.188.65.185:5432/postgres?user=postgres&password=control-postgres&currentSchema=public"))
   global.impalaSetting.append(new HuemulKeyValuePath("experimental","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))

   //TEMPORAL SETTING
   global.temporalPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/temp/"))
   global.temporalPath.append(new HuemulKeyValuePath("experimental","hdfs://hdfs:///user/data/experimental/temp/"))

   //RAW SETTING
   global.rawSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/raw/"))
   global.rawSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))

   global.rawBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/raw/"))
   global.rawBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))

   //BACKUP SETTING
   global.mdmBackupPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/backup/"))
   global.mdmBackupPath.append(new HuemulKeyValuePath("experimental","hdfs://hdfs:///user/data/experimental/backup/"))


   //MASTER SETTING
   global.masterDataBase.append(new HuemulKeyValuePath("production","production_master"))
   global.masterDataBase.append(new HuemulKeyValuePath("experimental","experimental_master"))

   global.masterSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/master/"))
   global.masterSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/master/"))

   global.masterBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/master/"))
   global.masterBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/master/"))

   //DIM SETTING
   global.dimDataBase.append(new HuemulKeyValuePath("production","production_dim"))
   global.dimDataBase.append(new HuemulKeyValuePath("experimental","experimental_dim"))

   global.dimSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/dim/"))
   global.dimSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))

   global.dimBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/dim/"))
   global.dimBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))

   //ANALYTICS SETTING
   global.analyticsDataBase.append(new HuemulKeyValuePath("production","production_analytics"))
   global.analyticsDataBase.append(new HuemulKeyValuePath("experimental","experimental_analytics"))

   global.analyticsSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/analytics/"))
   global.analyticsSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))

   global.analyticsBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/analytics/"))
   global.analyticsBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))

   //REPORTING SETTING
   global.reportingDataBase.append(new HuemulKeyValuePath("production","production_reporting"))
   global.reportingDataBase.append(new HuemulKeyValuePath("experimental","experimental_reporting"))

   global.reportingSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/reporting/"))
   global.reportingSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))

   global.reportingBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/reporting/"))
   global.reportingBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))

   //SANDBOX SETTING
   global.sandboxDataBase.append(new HuemulKeyValuePath("production","production_sandbox"))
   global.sandboxDataBase.append(new HuemulKeyValuePath("experimental","experimental_sandbox"))

   global.sandboxSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   global.sandboxSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))

   global.sandboxBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   global.sandboxBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))

   //DQ_ERROR SETTING
   global.dqSaveErrorDetails = true
   global.dqErrorDataBase.append(new HuemulKeyValuePath("production","production_DQError"))
   global.dqErrorDataBase.append(new HuemulKeyValuePath("experimental","experimental_DQError"))

   global.dqErrorPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/dqerror/"))
   global.dqErrorPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/dqerror/"))

   //OLD VALUE TRACE
   global.mdmSaveOldValueTrace = true
   global.mdmOldValueTraceDataBase.append(new HuemulKeyValuePath("production","production_mdm_oldvalue"))
   global.mdmOldValueTraceDataBase.append(new HuemulKeyValuePath("experimental","experimental_mdm_oldvalue"))

   global.mdmOldValueTracePath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/mdm_oldvalue/"))
   global.mdmOldValueTracePath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/mdm_oldvalue/"))

   //BACKUP
   global.mdmBackupPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/backup/"))
   global.mdmBackupPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/backup/"))

}

