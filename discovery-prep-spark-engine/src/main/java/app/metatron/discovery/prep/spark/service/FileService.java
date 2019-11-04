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
import static app.metatron.discovery.prep.spark.service.PropertyConstant.HADOOP_CONF_DIR;

import app.metatron.discovery.prep.spark.util.CsvUtil;
import app.metatron.discovery.prep.spark.util.JsonUtil;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FileService {

  private static Logger LOGGER = LoggerFactory.getLogger(FileService.class);

  private String hadoopConfDir;
  private Configuration hadoopConf = null;
  private Integer limitRows = null;

  public void setPrepPropertiesInfo(Map<String, Object> prepPropertiesInfo) throws IOException {
    hadoopConfDir = (String) prepPropertiesInfo.get(HADOOP_CONF_DIR);
    limitRows = (Integer) prepPropertiesInfo.get(ETL_LIMIT_ROWS);

    if (hadoopConfDir != null) {
      hadoopConf = Util.getHadoopConf(hadoopConfDir);
    }
  }

  public long createSnapshot(Dataset<Row> df, Map<String, Object> snapshotInfo) throws URISyntaxException, IOException {
    long totalLines;

    LOGGER.info("FileService.createSnapshot(): start");

    String ssUri = (String) snapshotInfo.get("storedUri");
    String ssUriFormat = ssUri.endsWith(".json") ? "JSON" : "CSV";

    URI uri = new URI(ssUri);
    if (uri.getScheme() == null) {
      ssUri = "file://" + ssUri;
      uri = new URI(ssUri);
    }

    switch (uri.getScheme()) {
      case "file":
        if (ssUriFormat.equals("JSON")) {
          totalLines = JsonUtil.writeJson(df, ssUri, null, limitRows);
        } else {
          totalLines = CsvUtil.writeCsv(df, ssUri, null, limitRows);
        }
        break;

      case "hdfs":
        Configuration conf = new Configuration();
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));

        if (ssUriFormat.equals("JSON")) {
          totalLines = JsonUtil.writeJson(df, ssUri, conf, limitRows);
        } else {
          totalLines = CsvUtil.writeCsv(df, ssUri, conf, limitRows);
        }
        break;

      default:
        throw new IOException("Wrong uri scheme: " + uri);
    }

    LOGGER.info("FileService.createSnapshot(): end: totalLines={}", totalLines);
    return totalLines;
  }

  private Dataset<Row> renameAsColumnN(Dataset<Row> df) {
    for (int i = 0; i < df.columns().length; i++) {
      df = df.withColumnRenamed("_c" + i, "column" + (i + 1));
    }
    return df;
  }

  public Dataset<Row> createStage0(Map<String, Object> datasetInfo, boolean header)
          throws IOException, URISyntaxException {
    String storedUri = (String) datasetInfo.get("storedUri");
    Integer columnCount = (Integer) datasetInfo.get("manualColumnCount");
    String extensionType = FilenameUtils.getExtension(storedUri);

    // If not .json, treat as a CSV.
    switch (extensionType.toUpperCase()) {
      case "JSON":
        StructType schema = JsonUtil.getSchemaFromJson(storedUri);
        return SparkUtil.getSession().read().schema(schema).json(storedUri).limit(limitRows);
      default:
        String delimiter = (String) datasetInfo.get("delimiter");
        Dataset<Row> df = SparkUtil.getSession().read()
                .format("CSV")
                .option("delimiter", delimiter)
                .option("header", header)
                .load(storedUri).limit(limitRows);

        return header ? df : renameAsColumnN(df);
    }
  }
}
