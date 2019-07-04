package app.metatron.discovery.prep.spark.service;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Header;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.PrepTransformer;
import app.metatron.discovery.prep.spark.util.CsvUtil;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DiscoveryPrepSparkEngineService {

  private static Logger LOGGER = LoggerFactory.getLogger(DiscoveryPrepSparkEngineService.class);

  @Autowired
  JavaSparkContext sc;

  public Map<String, Object> parseRule(String ruleString) {
    Rule rule = new RuleVisitorParser().parse(ruleString);
    HashMap<String, Object> map = new HashMap();
    map.put("result", rule.toString());

    List<String> wordList = Arrays.asList(ruleString.split(" "));
    JavaRDD<String> words = sc.parallelize(wordList);
    Map<String, Long> wordCounts = words.countByValue();
    map.put("wordCount", wordCounts);

    return map;
  }

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

  public void run(Map<String, Object> args)
      throws URISyntaxException, IOException, AnalysisException {
    Map<String, Object> prepProperties = (Map<String, Object>) args.get("prepProperties");
    Map<String, Object> datasetInfo = (Map<String, Object>) args.get("datasetInfo");
    Map<String, Object> snapshotInfo = (Map<String, Object>) args.get("snapshotInfo");
    Map<String, Object> callbackInfo = (Map<String, Object>) args.get("callbackInfo");

    String appName = (String) prepProperties.get("polaris.dataprep.spark.appName");
    String masterUri = (String) prepProperties.get("polaris.dataprep.spark.master");

    String dsStrUri = (String) datasetInfo.get("storedUri");
    String delimiter = (String) datasetInfo.get("delimiter");
    List<String> ruleStrings = (List<String>) datasetInfo.get("ruleStrings");

    String ssStrUri = (String) snapshotInfo.get("storedUri");

    SparkUtil.appName = appName;
    SparkUtil.masterUri = masterUri;

    // Load
    Dataset<Row> df = SparkUtil.getSession().read().format("CSV").option("delimiter", delimiter)
        .option("header", removeUnusedRules(ruleStrings)).load(dsStrUri);

    // Transform
    PrepTransformer transformer = new PrepTransformer();

    for (String ruleString : ruleStrings) {
      df = transformer.applyRule(df, ruleString);
    }

    // Write
    URI uri = new URI(ssStrUri);
    if (uri.getScheme() == null) {
      ssStrUri = "file://" + ssStrUri;
      uri = new URI(ssStrUri);
    }

    switch (uri.getScheme()) {
      case "file":
        FileUtils.deleteDirectory(new File(ssStrUri));
        CsvUtil.writeCsvToLocal(df, ssStrUri);
        break;
      case "hdfs":
        df.coalesce(1).write().option("header", "true").csv(ssStrUri);
        break;
      default:
        LOGGER.error("Wrong uri scheme: " + uri);
    }
  }
}
