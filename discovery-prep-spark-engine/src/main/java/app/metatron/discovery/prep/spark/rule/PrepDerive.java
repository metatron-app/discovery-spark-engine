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

import app.metatron.discovery.prep.parser.preparation.rule.Derive;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
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

    while (contains(colNames, as)) {
      as = as + "_1";
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

  private boolean contains(String[] arr, String str) {
    for (String s : arr) {
      if (s.equals(str)) {
        return true;
      }
    }
    return false;
  }
}
