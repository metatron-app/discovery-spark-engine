package app.metatron.discovery.prep.spark;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.rule.PrepDrop;
import app.metatron.discovery.prep.spark.rule.PrepHeader;
import app.metatron.discovery.prep.spark.rule.PrepKeep;
import app.metatron.discovery.prep.spark.rule.PrepRename;
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
        return PrepRename.transform(df, rule);
      case "header":
        return PrepHeader.transform(df, rule);
      case "drop":
        return PrepDrop.transform(df, rule);
      case "keep":
        return PrepKeep.transform(df, rule);
    }

    assert false : ruleString;
    return null;
  }
}
