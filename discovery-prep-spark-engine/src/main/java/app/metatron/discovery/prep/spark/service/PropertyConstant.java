package app.metatron.discovery.prep.spark.service;

public class PropertyConstant {
  public static final String LOCAL_BASE_DIR = "polaris.dataprep.localBaseDir";
  public static final String HADOOP_CONF_DIR = "polaris.dataprep.hadoopConfDir";
  public static final String STAGING_BASE_DIR = "polaris.dataprep.stagingBaseDir";
  public static final String S3_BASE_DIR = "polaris.dataprep.s3BaseDir";

  public static final String SAMPLING_CORES = "polaris.dataprep.sampling.cores";
  public static final String SAMPLING_TIMEOUT = "polaris.dataprep.sampling.timeout";
  public static final String SAMPLING_LIMIT_ROWS = "polaris.dataprep.sampling.limitRows";
  public static final String SAMPLING_MAX_FETCH_SIZE = "polaris.dataprep.sampling.maxFetchSize";
  public static final String SAMPLING_AUTO_TYPING = "polaris.dataprep.sampling.autoTyping";

  public static final String STAGEDB_HOSTNAME = "polaris.storage.stagedb.hostname";
  public static final String STAGEDB_PORT = "polaris.storage.stagedb.port";
  public static final String STAGEDB_USERNAME = "polaris.storage.stagedb.username";
  public static final String STAGEDB_PASSWORD = "polaris.storage.stagedb.password";
  public static final String STAGEDB_METASTORE_URI = "polaris.storage.stagedb.metastore.uri";

  public static final String ETL_CORES = "polaris.dataprep.etl.cores";
  public static final String ETL_TIMEOUT = "polaris.dataprep.etl.timeout";
  public static final String ETL_LIMIT_ROWS = "polaris.dataprep.etl.limitRows";
  public static final String ETL_MAX_FETCH_SIZE = "polaris.dataprep.etl.maxFetchSize";
  public static final String ETL_JVM_OPTIONS = "polaris.dataprep.etl.jvmOptions";
  public static final String ETL_EXPLICIT_GC = "polaris.dataprep.etl.explicitGC";

  public static final String ETL_SPARK_LIMIT_ROWS = "polaris.dataprep.etl.spark.limitRows";
  public static final String ETL_SPARK_HOSTNAME = "polaris.dataprep.etl.spark.hostname";
  public static final String ETL_SPARK_APP_NAME = "polaris.dataprep.etl.spark.appName";
  public static final String ETL_SPARK_MASTER = "polaris.dataprep.etl.spark.master";
  public static final String ETL_SPARK_WAREHOUSE_DIR = "polaris.dataprep.etl.spark.warehouseDir";

  public static final String STORAGE_STAGEDB_METASTORE_URI = "polaris.storage.stagedb.metastore.uri";
}
