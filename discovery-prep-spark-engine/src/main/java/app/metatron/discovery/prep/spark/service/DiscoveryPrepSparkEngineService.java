package app.metatron.discovery.prep.spark.service;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
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
    return map;
  }
}
