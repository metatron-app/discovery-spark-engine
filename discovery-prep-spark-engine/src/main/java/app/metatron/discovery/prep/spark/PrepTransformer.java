package app.metatron.discovery.prep.spark;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.rule.PrepDelete;
import app.metatron.discovery.prep.spark.rule.PrepDerive;
import app.metatron.discovery.prep.spark.rule.PrepDrop;
import app.metatron.discovery.prep.spark.rule.PrepHeader;
import app.metatron.discovery.prep.spark.rule.PrepKeep;
import app.metatron.discovery.prep.spark.rule.PrepRename;
import app.metatron.discovery.prep.spark.rule.PrepReplace;
import app.metatron.discovery.prep.spark.rule.PrepSet;
import app.metatron.discovery.prep.spark.rule.PrepSetType;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PrepTransformer {

  SparkSession session;
  RuleVisitorParser parser;

  public PrepTransformer() {
    session = SparkUtil.getSession();
    parser = new RuleVisitorParser();
  }

  public Dataset<Row> applyRule(Dataset<Row> df, String ruleString) throws AnalysisException {
    Rule rule = parser.parse(ruleString);

    switch (rule.getName()) {
      case "rename":
        return (new PrepRename()).transform(df, rule);
      case "header":
        return (new PrepHeader()).transform(df, rule);
      case "drop":
        return (new PrepDrop()).transform(df, rule);
      case "keep":
        return (new PrepKeep()).transform(df, rule);
      case "delete":
        return (new PrepDelete()).transform(df, rule);
      case "settype":
        return (new PrepSetType()).transform(df, rule);
      case "set":
        return (new PrepSet()).transform(df, rule);
      case "derive":
        return (new PrepDerive()).transform(df, rule);
      case "replace":
        return (new PrepReplace()).transform(df, rule);
    }

    assert false : ruleString;
    return null;
  }
}
