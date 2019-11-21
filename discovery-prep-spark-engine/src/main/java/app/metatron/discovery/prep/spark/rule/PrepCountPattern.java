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

import app.metatron.discovery.prep.parser.preparation.rule.CountPattern;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepCountPattern extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    CountPattern countPattern = (CountPattern) rule;
    Expression col = countPattern.getCol();
    Expression on = countPattern.getOn();
    Boolean ignoreCase = countPattern.getIgnoreCase();
    Expression quote = countPattern.getQuote();

    SparkUtil.createTempView(df, "temp");

    String patternStr = getPatternStr(on, ignoreCase);
    String quoteStr = getQuoteStr(quote);
    patternStr = modifyPatternStrWithQuote(patternStr, quoteStr);

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();

    int lastColno = -1;
    for (int i = 0; i < df.columns().length; i++) {
      if (targetColNames.contains(colNames[i])) {
        lastColno = i;
      }
    }
    assert lastColno >= 0 : countPattern;

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      String colName = colNames[i];
      sql = String.format("%s`%s`, ", sql, colName);

      if (i != lastColno) {
        continue;
      }

      String countExpr = "";
      String newColName = "countpattern_";

      for (String targetColName : targetColNames) {
        countExpr = String
                .format("%scount_pattern_ex(`%s`, '%s', '%s') + ", countExpr, targetColName, patternStr, quoteStr);
        newColName += targetColName + "_";
      }
      countExpr = countExpr.substring(0, countExpr.length() - 3);
      newColName = newColName.substring(0, newColName.length() - 1);

      sql = String.format("%s%s AS %s, ", sql, countExpr, newColName);
    }

    sql = sql.substring(0, sql.length() - 2) + " FROM temp";
    return SparkUtil.getSession().sql(sql);
  }
}
