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

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Header;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.PrepTransformer;
import app.metatron.discovery.prep.spark.util.Callback;
import app.metatron.discovery.prep.spark.util.CsvUtil;
import app.metatron.discovery.prep.spark.util.JsonUtil;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DiscoveryPrepSparkEngineService {

  private static Logger LOGGER = LoggerFactory.getLogger(DiscoveryPrepSparkEngineService.class);

  String appName;
  String masterUri;
  String warehouseDir;
  String metastoreUris;

  Map<String, Object> datasetInfo;
  List<String> ruleStrings;
  List<Map<String, Object>> upstreamDatasetInfos;
  int ruleCntTotal;

  String ssId;
  String ssName;
  String ssType;
  String ssUri;
  String ssUriFormat;
  String dbName;
  String tblName;

  String launchTime;

  Callback callback;

  private boolean removeUnusedRules(List<String> ruleStrings) {
    if (ruleStrings.size() > 0 && ruleStrings.get(0).startsWith("create")) {
      ruleStrings.remove(0);
    }

    if (ruleStrings.size() > 0) {
      String ruleString = ruleStrings.get(0);
      Rule rule = (new RuleVisitorParser()).parse(ruleString);

      if (rule instanceof Header) {
        Header header = (Header) rule;
        if (header.getRownum() == null || header.getRownum().longValue() == 1) {
          ruleStrings.remove(0);
          return true;
        }
      }
    }
    return false;
  }

  private Dataset<Row> createStage0(Map<String, Object> datasetInfo)
          throws IOException, URISyntaxException {
    String importType = (String) datasetInfo.get("importType");
    String dbName = (String) datasetInfo.get("dbName");
    String tblName = (String) datasetInfo.get("tblName");
    List<String> ruleStrings = (List<String>) datasetInfo.get("ruleStrings");

    switch (importType) {
      case "UPLOAD":
      case "URI":
        String storedUri = (String) datasetInfo.get("storedUri");
        Integer columnCount = (Integer) datasetInfo.get("manualColumnCount");
        String extensionType = FilenameUtils.getExtension(storedUri);

        // If not .json, treat as a CSV.
        switch (extensionType.toUpperCase()) {
          case "JSON":
            StructType schema = JsonUtil.getSchemaFromJson(storedUri);
            return SparkUtil.getSession().read().schema(schema).json(storedUri);
          default:
            String delimiter = (String) datasetInfo.get("delimiter");
            return SparkUtil.getSession().read().format("CSV").option("delimiter", delimiter)
                    .option("header", removeUnusedRules(ruleStrings)).load(storedUri);
        }

      case "STAGING_DB":
        return SparkUtil.selectTableAll(dbName, tblName);

      case "DATABASE":
      default:
        throw new IOException("Wrong importType: " + importType);
    }
  }

  private int countAllRules(Map<String, Object> datasetInfo) {
    upstreamDatasetInfos = (List<Map<String, Object>>) datasetInfo.get("upstreamDatasetInfos");
    if (upstreamDatasetInfos == null) {
      return ruleStrings.size();
    }

    for (Map<String, Object> upstreamDatasetInfo : upstreamDatasetInfos) {
      ruleCntTotal += countAllRules(upstreamDatasetInfo);
    }
    return ruleCntTotal + ((List<String>) datasetInfo.get("ruleStrings")).size();
  }

  public void setArgs(Map<String, Object> args) {
    Map<String, Object> prepProperties = (Map<String, Object>) args.get("prepProperties");
    Map<String, Object> snapshotInfo = (Map<String, Object>) args.get("snapshotInfo");
    Map<String, Object> callbackInfo = (Map<String, Object>) args.get("callbackInfo");

    datasetInfo = (Map<String, Object>) args.get("datasetInfo");

    appName = (String) prepProperties.get("polaris.dataprep.etl.spark.appName");
    masterUri = (String) prepProperties.get("polaris.dataprep.etl.spark.master");
    warehouseDir = (String) prepProperties.get("polaris.dataprep.etl.spark.warehouseDir");
    metastoreUris = (String) prepProperties.get("polaris.storage.stagedb.metastore.uri");

    ruleStrings = (List<String>) datasetInfo.get("ruleStrings");
    ruleCntTotal = countAllRules(datasetInfo);

    ssId = (String) snapshotInfo.get("ssId");
    ssName = (String) snapshotInfo.get("ssName");
    ssType = (String) snapshotInfo.get("ssType");
    switch (ssType) {
      case "URI":
        ssUri = (String) snapshotInfo.get("storedUri");
        ssUriFormat = ssUri.endsWith(".json") ? "JSON" : "CSV";
        break;
      case "STAGING_DB":
        dbName = (String) snapshotInfo.get("dbName");
        tblName = (String) snapshotInfo.get("tblName");
        break;
      default:
        assert false : ssType;
    }
    launchTime = (String) snapshotInfo.get("launchTime");

    LOGGER.info("setArgs(): ssName={} launchTime={} ssType={}", ssName, launchTime, ssType);

    callback = new Callback(callbackInfo, ssId);
  }

  public void run(Map<String, Object> args) throws AnalysisException, IOException, URISyntaxException {
    LOGGER.info("run(): started");

    setArgs(args);

    SparkUtil.setAppName(appName);
    SparkUtil.setMasterUri(masterUri);

    if (metastoreUris != null) {
      SparkUtil.setMetastoreUris(metastoreUris);
      assert warehouseDir != null;
      SparkUtil.setWarehouseDir(warehouseDir);
    }

    callback.updateSnapshot("ruleCntTotal", String.valueOf(ruleCntTotal), ssId);
    callback.updateAsRunning(ssId);

    try {
      // Load dataset
      Dataset<Row> df = createStage0(datasetInfo);
      long totalLines = 0L;

      // Transform dataset
      PrepTransformer transformer = new PrepTransformer();
      for (String ruleString : ruleStrings) {
        df = transformer.applyRule(df, ruleString);
        callback.incrRuleCntDone(ssId);
      }

      // Write as snapshot
      switch (ssType) {
        case "URI":
          URI uri = new URI(ssUri);
          if (uri.getScheme() == null) {
            ssUri = "file://" + ssUri;
            uri = new URI(ssUri);
          }

          switch (uri.getScheme()) {
            case "file":
              callback.updateAsWriting(ssId);

              if (ssUriFormat.equals("JSON")) {
                totalLines = JsonUtil.writeJson(df, ssUri, null);
              } else {
                totalLines = CsvUtil.writeCsv(df, ssUri, null);
              }
              break;

            case "hdfs":
              Configuration conf = new Configuration();
              conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));

              callback.updateAsWriting(ssId);

              if (ssUriFormat.equals("JSON")) {
                totalLines = JsonUtil.writeJson(df, ssUri, conf);
              } else {
                totalLines = CsvUtil.writeCsv(df, ssUri, conf);
              }
              break;

            default:
              throw new IOException("Wrong uri scheme: " + uri);
          }

          break;

        case "STAGING_DB":
          assert metastoreUris != null;

          callback.updateAsTableCreating(ssId);
          SparkUtil.createTable(df, dbName, tblName);
          totalLines = df.count();
          break;

        default:
          throw new IOException("Wrong ssType: " + ssType);
      }

      callback.updateSnapshot("totalLines", String.valueOf(totalLines), ssId);

    } catch (CancellationException ce) {
      LOGGER.info("run(): snapshot canceled from run_internal(): ", ce);

      String finishTime = DateTime.now(DateTimeZone.UTC).toString();
      callback.updateSnapshot("finishTime", finishTime, ssId);
      callback.updateAsCanceled(ssId);
      LOGGER.info("run(): result=CANCELED finishTime={}", finishTime);

      StringBuffer sb = new StringBuffer();

      for (StackTraceElement ste : ce.getStackTrace()) {
        sb.append("\n");
        sb.append(ste.toString());
      }
      callback.updateSnapshot("custom", "{'fail_msg':'" + sb.toString() + "'}", ssId);
      throw ce;
    } catch (Exception e) {
      LOGGER.error("run(): error while creating a snapshot: ", e);

      String finishTime = DateTime.now(DateTimeZone.UTC).toString();
      callback.updateSnapshot("finishTime", finishTime, ssId);
      callback.updateAsFailed(ssId);
      LOGGER.info("run(): result=FAILED finishTime={}", finishTime);

      StringBuffer sb = new StringBuffer();

      for (StackTraceElement ste : e.getStackTrace()) {
        sb.append("\n");
        sb.append(ste.toString());
      }
      callback.updateSnapshot("custom", "{'fail_msg':'" + sb.toString() + "'}", ssId);
      throw e;
    }

    // Why should we write colDescs at the end of snapshot generation?
    // It could be done by the start when the snapshot informations are filled, like ssName, ssType, etc.
    callback.updateSnapshot("custom", "Not implemented in spark engine", ssId);   // colDescs

    String finishTime = DateTime.now(DateTimeZone.UTC).toString();
    callback.updateSnapshot("finishTime", finishTime, ssId);
    LOGGER.info("run(): result=SUCCEEDED finishTime={}", finishTime);
    callback.updateAsSucceeded(ssId);
  }
}
