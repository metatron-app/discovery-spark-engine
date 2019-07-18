package app.metatron.discovery.prep.spark.service;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Header;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.PrepTransformer;
import app.metatron.discovery.prep.spark.util.CsvUtil;
import app.metatron.discovery.prep.spark.util.JsonUtil;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DiscoveryPrepSparkEngineService {

  private static Logger LOGGER = LoggerFactory.getLogger(DiscoveryPrepSparkEngineService.class);

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

  public void run(Map<String, Object> args)
      throws URISyntaxException, IOException, AnalysisException {
    Map<String, Object> prepProperties = (Map<String, Object>) args.get("prepProperties");
    Map<String, Object> datasetInfo = (Map<String, Object>) args.get("datasetInfo");
    Map<String, Object> snapshotInfo = (Map<String, Object>) args.get("snapshotInfo");
    Map<String, Object> callbackInfo = (Map<String, Object>) args.get("callbackInfo");

    String appName = (String) prepProperties.get("polaris.dataprep.spark.appName");
    String masterUri = (String) prepProperties.get("polaris.dataprep.spark.master");
    String metastoreUris = (String) prepProperties.get("polaris.storage.stagedb.metastore.uri");
    String warehouseDir = (String) prepProperties.get("polaris.dataprep.spark.warehouseDir");
    String defaultFS = (String) prepProperties.get("polaris.dataprep.spark.defaultFS");

    List<String> ruleStrings = (List<String>) datasetInfo.get("ruleStrings");

    String ssType = (String) snapshotInfo.get("ssType");
    String ssUri = (String) snapshotInfo.get("storedUri");
    String ssFormat = (String) snapshotInfo.get("format");
    String dbName = (String) snapshotInfo.get("dbName");
    String tblName = (String) snapshotInfo.get("tblName");

    SparkUtil.setAppName(appName);
    SparkUtil.setMasterUri(masterUri);
    SparkUtil.setDefaultFS(defaultFS);

    if (metastoreUris != null) {
      SparkUtil.setMetastoreUris(metastoreUris);
      assert warehouseDir != null;
      SparkUtil.setWarehouseDir(warehouseDir);
    }

    // Load dataset
    Dataset<Row> df = createStage0(datasetInfo);

    // Transform dataset
    PrepTransformer transformer = new PrepTransformer();
    for (String ruleString : ruleStrings) {
      df = transformer.applyRule(df, ruleString);
    }

    // Write as snapshot
    switch (ssType) {
      case "LOCAL":
        URI uri = new URI(ssUri);
        if (uri.getScheme() == null) {
          ssUri = "file://" + ssUri;
          uri = new URI(ssUri);
        }

        switch (uri.getScheme()) {
          case "file":
            if (ssFormat.equals("JSON")) {
              JsonUtil.writeJson(df, ssUri, null);
            } else {
              CsvUtil.writeCsv(df, ssUri, null);
            }
            break;

          case "hdfs":
            Configuration conf = new Configuration();
            conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));

            if (ssFormat.equals("JSON")) {
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
        SparkUtil.createTable(df, dbName, tblName);
        break;

      default:
        throw new IOException("Wrong ssType: " + ssType);
    }

    SparkUtil.stopSession();
  }
}
