package app.metatron.discovery.prep.spark.service;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.SparkUtil;
import app.metatron.discovery.prep.spark.rule.PrepRename;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DiscoveryPrepSparkEngineService {

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

  public Map<String, Object> run(Map<String, Object> args) {
    Map<String, Object> prepProperties = (Map<String, Object>) args.get("prepProperties");
    Map<String, Object> datasetInfo = (Map<String, Object>) args.get("datasetInfo");
    Map<String, Object> snapshotInfo = (Map<String, Object>) args.get("snapshotInfo");
    Map<String, Object> callbackInfo = (Map<String, Object>) args.get("callbackInfo");

    String appName = (String) prepProperties.get("polaris.dataprep.spark.appName");
    String masterUri = (String) prepProperties.get("polaris.dataprep.spark.master");

    String storedUri = (String) datasetInfo.get("storedUri");
    String delimiter = (String) datasetInfo.get("delimiter");
    List<String> ruleStrings = (List<String>) datasetInfo.get("ruleStrings");
    String ruleString = ruleStrings.get(0);

    String localBaseDir = (String) snapshotInfo.get("localBaseDir");

    SparkUtil.appName = appName;
    SparkUtil.masterUri = masterUri;

    Dataset<Row> df = SparkUtil.getSession().read().format("CSV").option("delimiter", delimiter)
        .load(storedUri);
    PrepRename prepRename = new PrepRename();
    prepRename.parse(ruleString);
    df = prepRename.transform(df);

    try {
      FileUtils.deleteDirectory(new File(localBaseDir + "/spark.result.csv"));
    } catch (IOException e) {
      e.printStackTrace();
    }

    df.coalesce(1).write().option("header", "true").csv(localBaseDir + "/spark.result.csv");

    Map<String, Object> result = new HashMap();
    result.put("result", "OK");
    return result;
  }
}
