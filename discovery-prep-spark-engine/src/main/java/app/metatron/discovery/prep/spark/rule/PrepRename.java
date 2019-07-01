package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Rename;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepRename extends PrepRule {

  private Rename rename;
  private Expression col;
  private Expression to;

  public PrepRename() {
  }

  public void parse(String ruleString) {
    RuleVisitorParser parser = new RuleVisitorParser();
    rename = (Rename) parser.parse(ruleString);
    col = rename.getCol();
    to = rename.getTo();
  }

  public Dataset<Row> transform(Dataset<Row> df) {
    List<String> colNames = getIdentifierList(col);
    List<String> toNames = getIdentifierList(to);
    Dataset<Row> newDf = df;

    for (int i = 0; i < colNames.size(); i++) {
      newDf = newDf.withColumnRenamed(colNames.get(i), toNames.get(i));
    }

    return newDf;
  }
}
