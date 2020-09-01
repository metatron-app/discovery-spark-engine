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

package app.metatron.discovery.prep.spark.jdbc;

import app.metatron.discovery.prep.spark.service.DatabaseService;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;

public class JdbcTest {

  @BeforeClass
  public static void setup() {
//    SparkUtil.setAppName("DiscoverySparkEngine");
//    SparkUtil.setMasterUri("local");
//    SparkUtil.setWarehouseDir("hdfs://localhost:9000/user/hive/warehouse");
//    SparkUtil.setMetastoreUris("thrift://localhost:9083");
  }

  //  @Test
  public void testMySQL() throws IOException, AnalysisException, URISyntaxException, ClassNotFoundException {
    DatabaseService databaseService = new DatabaseService();

    Map<String, Object> dsInfo = new HashMap();
    dsInfo.put("connectUri", "jdbc:mysql://localhost:3306");
    dsInfo.put("username", "polaris");
    dsInfo.put("password", "polaris");
    dsInfo.put("sourceQuery", "select * from test.t");

    Dataset<Row> df = databaseService.createStage0(dsInfo);
    df.show();
  }

  //  @Test
  public void testPostgreSQL() throws IOException, AnalysisException, URISyntaxException, ClassNotFoundException {
    DatabaseService databaseService = new DatabaseService();

    Map<String, Object> dsInfo = new HashMap();
    dsInfo.put("connectUri", "jdbc:postgresql://localhost:5432/testdb");
    dsInfo.put("username", "jhkim");
    dsInfo.put("password", "jhkim");
    dsInfo.put("sourceQuery", "select * from s2.t2");

    Dataset<Row> df = databaseService.createStage0(dsInfo);
    df.show();
  }
}

