package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.IdentifierArrayExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.IdentifierExpr;
import app.metatron.discovery.prep.spark.SparkUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PrepRule {

  private SparkSession session;

  public PrepRule() {
    session = SparkUtil.getSession();
  }

  // Override this
  public Dataset<Row> transform(Dataset<Row> df) {
    return df;
  }

  public List<String> getIdentifierList(Expression expr) {
    List<String> arr = new ArrayList();

    if (expr instanceof IdentifierExpr) {
      arr.add(((IdentifierExpr) expr).getValue());
    } else {
      for (String identifier : ((IdentifierArrayExpr) expr).getValue()) {
        arr.add(identifier);
      }
    }

    return arr;
  }
}
