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

package app.metatron.discovery.prep.spark.service;

import static app.metatron.discovery.prep.spark.service.PropertyConstant.ETL_LIMIT_ROWS;

import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DatabaseService {

  private static Logger LOGGER = LoggerFactory.getLogger(DatabaseService.class);

  private Integer limitRows = null;

  public void setPrepPropertiesInfo(Map<String, Object> prepPropertiesInfo) throws IOException {
    limitRows = (Integer) prepPropertiesInfo.get(ETL_LIMIT_ROWS);
  }

  public long createSnapshot(Dataset<Row> df, Map<String, Object> snapshotInfo) throws URISyntaxException, IOException {
    LOGGER.info("DatabaseService.createSnapshot(): started");

    // NOTE:
    // Currently, Metatron Discovery doesn't support DB snapshots.
    // This function is only for tests for a while.
    String connectUri = (String) snapshotInfo.get("connectUri");
    String username = (String) snapshotInfo.get("username");
    String password = (String) snapshotInfo.get("password");
    String dbName = (String) snapshotInfo.get("dbName");
    String tblName = (String) snapshotInfo.get("tblName");

    SparkUtil.createTable(df, dbName, tblName, limitRows);
    long totalLines = df.count();

    LOGGER.info("DatabaseService.createSnapshot() finished: totalLines={}", totalLines);
    return totalLines;
  }

  public Dataset<Row> createStage0(Map<String, Object> datasetInfo) throws IOException, URISyntaxException {
    String connectUri = (String) datasetInfo.get("connectUri");
    String username = (String) datasetInfo.get("username");
    String password = (String) datasetInfo.get("password");
    String dbName = (String) datasetInfo.get("dbName");
    String tblName = (String) datasetInfo.get("tblName");

    return SparkUtil.getSession().read()
            .format("jdbc")
            .option("url", connectUri)
            .option("user", username)
            .option("password", password)
            .option("dbtable", dbName + "." + tblName)
            .load();
  }
}
