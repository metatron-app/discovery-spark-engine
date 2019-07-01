package app.metatron.discovery.prep.spark;

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
}
