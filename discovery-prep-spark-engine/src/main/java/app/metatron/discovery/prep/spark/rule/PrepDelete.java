package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Delete;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepDelete extends PrepRule {

  public static Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Delete delete = (Delete) rule;
    Expression row = delete.getRow();

    SparkUtil.createView(df, "temp");

    String sql = "SELECT * FROM temp WHERE NOT (" + asSparkExpr(row.toString()) + ")";
    return SparkUtil.getSession().sql(sql);
  }
}
