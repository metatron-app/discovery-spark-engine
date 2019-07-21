package app.metatron.discovery.prep.spark.util;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

public class SparkUtil {

  private static SparkSession session;

  private static String appName;
  private static String masterUri;
  private static String metastoreUris;
  private static String warehouseDir;

  public static SparkSession getSession() {
    if (session == null) {
      Builder builder = SparkSession.builder()
          .appName(appName)
          .master(masterUri);

      if (metastoreUris != null) {
        assert warehouseDir != null : metastoreUris;

        builder = builder
            .config("spark.sql.warehouse.dir", warehouseDir)
            .config("spark.sql.catalogImplementation", "hive")
            .config("hive.metastore.uris", metastoreUris)
            .enableHiveSupport();
      }
      session = builder.getOrCreate();
    }

    return session;
  }

  public static void stopSession() {
    if (session != null) {
      session.stop();
      session = null;
    }
  }

  public static void createTempView(Dataset<Row> df, String tempViewName) {
    df.createOrReplaceTempView(tempViewName);
  }

  public static void prepareCreateTable(Dataset<Row> df, String fullName) throws AnalysisException {
    createTempView(df, "temp");
    getSession().sql(String.format("DROP TABLE %s PURGE", fullName));
  }

  public static void createTable(Dataset<Row> df, String dbName, String tblName) {
    String fullName = dbName + "." + tblName;
    try {
      prepareCreateTable(df, fullName);
    } catch (AnalysisException e) {
      // NOTE:
      // This is a trick to catch "table not found" exception from DROP TABLE statement
      // The compiler knows DROP TABLE does not throw AnalysisException. (Why?)
    }
    getSession().sql(String.format("CREATE TABLE %s AS SELECT * FROM %s", fullName, "temp"));
  }

  public static Dataset<Row> selectTableAll(String dbName, String tblName) {
    String fullName = dbName + "." + tblName;
    return getSession().sql(String.format("SELECT * FROM %s", fullName));
  }

  public static void setAppName(String appName) {
    SparkUtil.appName = appName;
  }

  public static void setMasterUri(String masterUri) {
    SparkUtil.masterUri = masterUri;
  }

  public static void setMetastoreUris(String metastoreUris) {
    SparkUtil.metastoreUris = metastoreUris;
  }

  public static void setWarehouseDir(String warehouseDir) {
    SparkUtil.warehouseDir = warehouseDir;
  }
}
