package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.SetType;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

public class PrepSetType extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    SetType settype = (SetType) rule;
    Expression col = settype.getCol();
    String toType = settype.getType().toLowerCase();
    String format = settype.getFormat();

    SparkUtil.createTempView(df, "temp");

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();
    StructField[] fields = df.schema().fields();

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      String colName = colNames[i];

      if (!targetColNames.contains(colName)) {
        sql = sql + "`" + colName + "`, ";
        continue;
      }

      String fromType = fields[i].dataType().typeName();

      switch (toType) {
        case "timestamp":
          sql = String
              .format("%sCAST(UNIX_TIMESTAMP(`%s`, '%s') AS TIMESTAMP) AS `%s`, ", sql, colName, format,
                  colName);
          break;
        case "string":
          if (fromType.equalsIgnoreCase("timestamp")) {
            sql = String
                .format("%sFROM_UNIXTIME(UNIX_TIMESTAMP(`%s`), '%s') AS `%s`, ", sql, colName, format,
                    colName);
          }
        default:
          sql = String.format("%sCAST(`%s` AS %s) AS `%s`, ", sql, colName, toType, colName);
          break;
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";

    return SparkUtil.getSession().sql(sql);
  }
}
