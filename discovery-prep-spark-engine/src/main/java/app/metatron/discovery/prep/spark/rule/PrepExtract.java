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

import app.metatron.discovery.prep.parser.preparation.rule.Extract;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.StringExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.parser.preparation.rule.expr.RegularExpr;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepExtract extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Extract extract = (Extract) rule;
    Expression col = extract.getCol();
    Expression on = extract.getOn();
    Integer limit = extract.getLimit();
    Expression quote = extract.getQuote();

    SparkUtil.createTempView(df, "temp");

    List<String> targetColNames = getIdentifierList(col);
    String[] colNames = df.columns();

    String patternStr;

    if (on instanceof RegularExpr) {
      patternStr = ((RegularExpr) on).getEscapedValue();
    } else {
      patternStr = ((StringExpr) on).getEscapedValue();
    }

    String quoteStr;
    if (quote == null) {
      quoteStr = null;
    } else {
      assert quote instanceof StringExpr : quote;
      quoteStr = ((StringExpr) quote).getEscapedValue();
    }

    String sql = "SELECT ";
    for (int i = 0; i < colNames.length; i++) {
      String colName = colNames[i];
      sql = String.format("%s`%s`, ", sql, colName);

      if (!targetColNames.contains(colName)) {
        continue;
      }

      for (int j = 0; j < limit; j++) {
        sql = String.format("%sregexp_extract_ex(`%s`, '%s', %d, %s) AS `extract_%s_%d`, ",
                sql, colName, patternStr, j, quoteStr, colName, j + 1);
      }
    }

    sql = sql.substring(0, sql.length() - 2) + " FROM temp";
    return SparkUtil.getSession().sql(sql);
  }
}
