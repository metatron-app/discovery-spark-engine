package app.metatron.discovery.prep.spark.util;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.hive.HiveSessionStateBuilder;

public class SparkUtil {

  private static SparkSession session;

  private static String appName;
  private static String masterUri;
  private static String metastoreUris;
  private static String warehouseDir;
  private static String defaultFS;

  public static SparkSession getSession() {
    if (session == null) {
      Builder builder = SparkSession.builder()
          .appName(appName)
          .master(masterUri);

      if (metastoreUris != null) {
        assert warehouseDir != null : metastoreUris;

        builder = builder
            .config("spark.hadoop.fs.defaultFS", defaultFS)
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

  // FIXME: Useless wrapper
  public static void createView(Dataset<Row> df, String viewName) throws AnalysisException {
    df.createOrReplaceTempView(viewName);
  }

  public static void createTable(Dataset<Row> df, String dbName, String tblName)
      throws AnalysisException {
    String tempViewName = tblName + "_temp_view";
    createView(df, tempViewName);

    String fullName = dbName + "." + tblName;
    getSession().sql(String.format("DROP TABLE IF EXISTS %s", fullName));
    getSession().sql(String.format("CREATE TABLE %s AS SELECT * FROM %s", fullName, tempViewName));
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

  public static void setDefaultFS(String defaultFS) {
    SparkUtil.defaultFS = defaultFS;
  }
}
