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

import static app.metatron.discovery.prep.spark.util.SparkUtil.getSession;

import app.metatron.discovery.prep.parser.preparation.rule.Nest;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class PrepNest extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException, IOException {
    Nest nest = (Nest) rule;
    Expression col = nest.getCol();
    String into = stripQuotes(nest.getInto());
    String as = nest.getAs();

    SparkUtil.createTempView(df, "temp");

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();
    int lastColno = getLastColno(targetColNames, colNames);

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      sql = String.format("%s%s, ", sql, colNames[i]);

      if (i == lastColno) {
        String colExpr = null;
        switch (into.toUpperCase()) {
          case "MAP":
            colExpr = "to_json(named_struct(";
            for (String targetColName : targetColNames) {
              colExpr = String.format("%s'%s', %s, ", colExpr, targetColName, targetColName);
            }
            colExpr = colExpr.substring(0, colExpr.length() - 2) + ")) AS " + as;
            break;
          case "ARRAY":

            String colNameArray = "array(";
            for (String targetColName : targetColNames) {
              colNameArray = String.format("%s%s, ", colNameArray, targetColName);
            }
            colNameArray = colNameArray.substring(0, colNameArray.length() - 2) + ")";

            String colTypeArray = "array(";
            for (String targetColName : targetColNames) {
              String typeStr = getTypeStr(df, targetColName);
              colTypeArray = String.format("%s'%s', ", colTypeArray, typeStr);
            }
            colTypeArray = colTypeArray.substring(0, colTypeArray.length() - 2) + ")";

            colExpr = String.format("array_to_json_ex(%s, %s) AS %s", colNameArray, colTypeArray, as);
            break;
          default:
            assert false : nest;
        }
        sql = String.format("%s%s, ", sql, colExpr);
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";
    return getSession().sql(sql);
  }

  private String getTypeStr(Dataset<Row> df, String colName) {
    StructType schema = df.schema();
    return schema.fields()[schema.fieldIndex(colName)].dataType().typeName();
  }
}
