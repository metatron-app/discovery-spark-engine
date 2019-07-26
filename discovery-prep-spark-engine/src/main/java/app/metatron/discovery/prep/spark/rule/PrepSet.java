package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.Set;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepSet extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Set set = (Set) rule;
    Expression col = set.getCol();
    Expression value = set.getValue();
    Expression row = set.getRow();

    SparkUtil.createTempView(df, "temp");

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      String colName = colNames[i];

      if (!targetColNames.contains(colName)) {
        sql = sql + colName + ", ";
        continue;
      }

      // FIXME: "$col" in literal case
      String strExpr = stringifyExpr(value).str.replace("$col", "`" + colName + "`");
      sql = String.format("%s%s AS %s, ", sql, strExpr, colName);
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";

    return SparkUtil.getSession().sql(sql);
  }
}
