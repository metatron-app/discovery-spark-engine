/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.metatron.discovery.prep.spark.util;

import java.net.URI;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(SparkUtil.class);

  private static SparkSession session;

  private static String appName;
  private static String masterUri;
  private static String metastoreUris;
  private static String warehouseDir;

  public static SparkSession getSession() {
    if (session == null) {
      LOGGER.info("session == null: creating session");
      LOGGER.info("appName={} masterUri={} spark.sql.warehouse.dir={}", appName, masterUri, warehouseDir);
      LOGGER.info("spark.sql.catalogImplementation={} hive.metastore.uris={}", "hive", metastoreUris);
      LOGGER.info("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation={}", "true");

      Builder builder = SparkSession.builder()
              .appName(appName)
              .master(masterUri);

      if (metastoreUris != null) {
        assert warehouseDir != null : metastoreUris;

        builder = builder
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.sql.catalogImplementation", "hive")
                .config("hive.metastore.uris", metastoreUris)
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
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

  private void deleteUri(URI uri) {
  }

  public static void prepareCreateTable(Dataset<Row> df, String dbName, String tblName) throws AnalysisException {
    createTempView(df, "temp");
    getSession().sql(String.format("DROP TABLE %s PURGE", dbName + "." + tblName));
  }

  public static void createTable(Dataset<Row> df, String dbName, String tblName) {
    try {
      prepareCreateTable(df, dbName, tblName);
    } catch (AnalysisException e) {
      // Suppress table not found
    }
    String fullName = dbName + "." + tblName;
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
