package app.metatron.discovery.prep.spark.util;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkUtil {

  private static SparkSession session;

  public static String appName;
  public static String masterUri;

  public static SparkSession getSession() {
    if (session == null) {
      session = SparkSession.builder().appName(appName).master(masterUri).getOrCreate();
    }
    return session;
  }

  public static Dataset<Row> createView(Dataset<Row> df, String viewName) throws AnalysisException {
    getSession().catalog().dropTempView(viewName);
    df.createTempView(viewName);
    return df;
  }
}
