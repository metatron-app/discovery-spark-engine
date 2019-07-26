package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Derive;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.Set;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepDerive extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Derive derive = (Derive) rule;
    Expression value = derive.getValue();
    String as = derive.getAs();

    SparkUtil.createTempView(df, "temp");

    StrExpResult result = stringifyExpr(value);
    String strExpr = result.str;

    String[] colNames = df.columns();

    int lastRelatedColno = colNames.length - 1;
    for (int i = 0; i < colNames.length; i++) {
      if (relatedColNames.contains(colNames[i])) {
        lastRelatedColno = i;
      }
    }

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      String colName = colNames[i];
      sql = sql + colName + ", ";

      if (i == lastRelatedColno) {
        sql = String.format("%s%s AS `%s`, ", sql, strExpr, as);
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";

    return SparkUtil.getSession().sql(sql);
  }
}
