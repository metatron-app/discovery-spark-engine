package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Replace;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.StringExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.parser.preparation.rule.expr.RegularExpr;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepReplace extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Replace replace = (Replace) rule;
    Expression col = replace.getCol();
    Expression on = replace.getOn();
    String with = (String) replace.getWith().getValue();

    SparkUtil.createTempView(df, "temp");

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();

    String pattern;

    if (on instanceof RegularExpr) {
      pattern = ((RegularExpr) on).getEscapedValue();
    } else {
      pattern = ((StringExpr) on).getEscapedValue();
    }

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      String colName = colNames[i];

      if (!targetColNames.contains(colName)) {
        sql = sql + colName + ", ";
        continue;
      }

      if (on instanceof RegularExpr) {
        sql = String.format("%sregexp_replace(`%s`, '%s', %s) AS `%s`, ", sql, colName, pattern, with, colName);
      } else {
        if (pattern.startsWith("'") && pattern.endsWith(("'"))) {
          pattern = pattern.substring(1, pattern.length() - 1);
        }
        sql = String.format("%sreplace(`%s`, '%s', %s) AS `%s`, ", sql, colName, pattern, with, colName);
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";

    return SparkUtil.getSession().sql(sql);
  }
}
