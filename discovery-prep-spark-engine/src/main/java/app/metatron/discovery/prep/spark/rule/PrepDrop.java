package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Drop;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepDrop extends PrepRule {

  public static Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Dataset<Row> newDf = df;

    Drop drop = (Drop) rule;
    Expression col = drop.getCol();

    List<String> colNames = getIdentifierList(col);

    for (int i = 0; i < colNames.size(); i++) {
      newDf = newDf.drop(colNames.get(i));
    }

    return newDf;
  }
}
