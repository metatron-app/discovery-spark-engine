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
  String ssType;
  String ssUri;
  String ssUriFormat;
  String dbName;
  String tblName;

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

        switch (extensionType.toUpperCase()) {
          case "CSV":
            String delimiter = (String) datasetInfo.get("delimiter");
            return SparkUtil.getSession().read().format("CSV").option("delimiter", delimiter)
                .option("header", removeUnusedRules(ruleStrings)).load(storedUri);
          case "JSON":
            StructType schema = JsonUtil.getSchemaFromJson(storedUri);
            return SparkUtil.getSession().read().schema(schema).json(storedUri);
          default:
            throw new IOException("Wrong extensionType: " + extensionType);
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

    callback = new Callback((Map<String, Object>) args.get("callbackInfo"), ssId);
  }

  public void run(Map<String, Object> args)
      throws AnalysisException, IOException, URISyntaxException {
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
                JsonUtil.writeJson(df, ssUri, null);
              } else {
                CsvUtil.writeCsv(df, ssUri, null);
              }
              break;

            case "hdfs":
              Configuration conf = new Configuration();
              conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));

              callback.updateAsWriting(ssId);

              if (ssUriFormat.equals("JSON")) {
                JsonUtil.writeJson(df, ssUri, conf);
              } else {
                CsvUtil.writeCsv(df, ssUri, conf);
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
          break;

        default:
          throw new IOException("Wrong ssType: " + ssType);
      }
    } catch (CancellationException ce) {
      LOGGER.info("run(): snapshot canceled from run_internal(): ", ce);
      callback.updateSnapshot("finishTime", DateTime.now().toString(), ssId);
      callback.updateAsCanceled(ssId);
      StringBuffer sb = new StringBuffer();

      for (StackTraceElement ste : ce.getStackTrace()) {
        sb.append("\n");
        sb.append(ste.toString());
      }
      callback.updateSnapshot("custom", "{'fail_msg':'" + sb.toString() + "'}", ssId);
      throw ce;
    } catch (Exception e) {
      LOGGER.error("run(): error while creating a snapshot: ", e);
      callback.updateSnapshot("finishTime", DateTime.now().toString(), ssId);
      callback.updateAsFailed(ssId);
      StringBuffer sb = new StringBuffer();

      for (StackTraceElement ste : e.getStackTrace()) {
        sb.append("\n");
        sb.append(ste.toString());
      }
      callback.updateSnapshot("custom", "{'fail_msg':'" + sb.toString() + "'}", ssId);
      throw e;
    }

    callback.updateSnapshot("custom", "Not implemented in spark engine", ssId);   // colDescs
    callback.updateSnapshot("totalLines", "Not implemented in spark engine", ssId);

    callback.updateSnapshot("finishTime", DateTime.now().toString(), ssId);
    callback.updateAsSucceeded(ssId);
  }
}
