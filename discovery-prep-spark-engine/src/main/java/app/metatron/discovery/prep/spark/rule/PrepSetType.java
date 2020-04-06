/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
                  .format("%sCAST(UNIX_TIMESTAMP(`%s`, '%s') AS TIMESTAMP) AS `%s`, ", sql, colName, format, colName);
          break;
        case "string":
          if (fromType.equalsIgnoreCase("timestamp")) {
            sql = String.format("%sFROM_UNIXTIME(UNIX_TIMESTAMP(`%s`), '%s') AS `%s`, ", sql, colName, format, colName);
          }
          break;
        case "array":
        case "map":
          assert fromType.equals("string");
          sql = String.format("%s`%s`, ", sql, colName);
          break;
        default:
          sql = String.format("%sCAST(`%s` AS %s) AS `%s`, ", sql, colName, getPhysicalType(toType), colName);
          break;
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";

    return SparkUtil.getSession().sql(sql);
  }

  private String getPhysicalType(String toType) {
    switch (toType) {
      case "map":
      case "array":
        return "string";
      default:
        return toType;
    }
  }
}
