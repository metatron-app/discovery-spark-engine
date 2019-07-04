package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Keep;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.SetType;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepSetType extends PrepRule {

  public static Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    SetType settype = (SetType) rule;
    Expression col = settype.getCol();
    String type = settype.getType().toLowerCase();
    String format = settype.getFormat();

    SparkUtil.createView(df, "temp");

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();

    String sql = "SELECT ";
    for (String colName : colNames) {
      if (!targetColNames.contains(colName)) {
        sql = sql + colName + ", ";
        continue;
      }

      switch (type) {
        case "timestamp":
          sql = String.format("%sTO_DATE(%s, %s) AS %s, ", sql, colName, format, colName);
          break;
        default:
          sql = String.format("%sCAST(%s AS %s) AS %s, ", sql, colName, type, colName);
          break;
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";

    return SparkUtil.getSession().sql(sql);
  }
}
