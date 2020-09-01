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

import app.metatron.discovery.prep.spark.udf.ArrayToJsonEx;
import app.metatron.discovery.prep.spark.udf.CountPatternEx;
import app.metatron.discovery.prep.spark.udf.FromArrayEx;
import app.metatron.discovery.prep.spark.udf.FromMapEx;
import app.metatron.discovery.prep.spark.udf.IsMismatchedEx;
import app.metatron.discovery.prep.spark.udf.IsNullEx;
import app.metatron.discovery.prep.spark.udf.RegexpExtractEx;
import app.metatron.discovery.prep.spark.udf.SplitEx;
import app.metatron.discovery.prep.spark.udf.ToArrayTypeEx;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(SparkUtil.class);

  private static SparkSession session;

  public static SparkSession getSession(String sparkAppName, String sparkMaster, String hiveMetastoreUris,
          String sparkSqlWarehouseDir, String sparkDriverMaxResultSize) {

    if (session == null) {
      LOGGER.info("creating session:");
      LOGGER.info("spark.app.name={} spark.master={}", sparkAppName, sparkMaster);

      LOGGER.info("spark.sql.warehouse.dir={} hive.metastore.uris={}", sparkSqlWarehouseDir, hiveMetastoreUris);
      boolean useHive = hiveMetastoreUris != null && sparkSqlWarehouseDir != null;

      LOGGER.info("spark.sql.catalogImplementation={}", useHive);
      LOGGER.info("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation={}", useHive);

      Builder builder = SparkSession.builder()
              .appName(sparkAppName)
              .master(sparkMaster);

      if (useHive) {
        builder = builder
                .config("hive.metastore.uris", hiveMetastoreUris)
                .config("spark.sql.warehouse.dir", sparkSqlWarehouseDir)
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
                .enableHiveSupport();
      } else {
        builder = builder
                .config("spark.sql.catalogImplementation", "in-memory");
      }
      builder = builder.config("spark.driver.maxResultSize", sparkDriverMaxResultSize);
      session = builder.getOrCreate();

      session.udf().register("split_ex", split_ex, DataTypes.StringType);
      session.udf().register("regexp_extract_ex", regexp_extract_ex, DataTypes.StringType);
      session.udf().register("count_pattern_ex", count_pattern_ex, DataTypes.IntegerType);
      session.udf().register("array_to_json_ex", array_to_json_ex, DataTypes.StringType);
      session.udf().register("isnull", is_null_ex, DataTypes.BooleanType);
      session.udf().register("ismismatched", is_mismatched_ex, DataTypes.BooleanType);
      session.udf().register("from_array_ex", from_array_ex, DataTypes.StringType);
      session.udf().register("from_map_ex", from_map_ex, DataTypes.StringType);
      session.udf().register("to_array_type_ex", to_array_type_ex, DataTypes.createArrayType(DataTypes.StringType));
    }

    return session;
  }

  public static SparkSession getSession() {
    if (session == null) {
      // Only for tests!
      // In a production environment, the session should be created with the above constructor.
      return getSession("testApp", "local", null, null, "1g");
    }

    return session;
  }

  private static SplitEx split_ex = new SplitEx();
  private static RegexpExtractEx regexp_extract_ex = new RegexpExtractEx();
  private static CountPatternEx count_pattern_ex = new CountPatternEx();
  private static ArrayToJsonEx array_to_json_ex = new ArrayToJsonEx();
  private static IsNullEx is_null_ex = new IsNullEx();
  private static IsMismatchedEx is_mismatched_ex = new IsMismatchedEx();
  private static FromMapEx from_map_ex = new FromMapEx();
  private static FromArrayEx from_array_ex = new FromArrayEx();
  private static ToArrayTypeEx to_array_type_ex = new ToArrayTypeEx();

  public static void stopSession() {
    if (session != null) {
      session.stop();
      session = null;
    }
  }

  public static void createTempView(Dataset<Row> df, String tempViewName) {
    df.createOrReplaceTempView(tempViewName);
  }

  public static void prepareCreateTable(Dataset<Row> df, String dbName, String tblName) throws AnalysisException {
    createTempView(df, "temp");
    getSession().sql(String.format("DROP TABLE %s PURGE", dbName + "." + tblName));
  }

  public static void createTable(Dataset<Row> df, String dbName, String tblName, int limitRows) {
    try {
      prepareCreateTable(df, dbName, tblName);
    } catch (AnalysisException e) {
      // Suppress "table not found"
    }
    String fullName = dbName + "." + tblName;
    getSession()
            .sql(String.format("CREATE TABLE %s USING ORC AS SELECT * FROM %s LIMIT %d", fullName, "temp", limitRows));
  }

  public static Dataset<Row> selectTableAll(String dbName, String tblName, int limitRows) {
    String fullName = dbName + "." + tblName;
    return getSession().sql(String.format("SELECT * FROM %s LIMIT %d", fullName, limitRows));
  }
}
