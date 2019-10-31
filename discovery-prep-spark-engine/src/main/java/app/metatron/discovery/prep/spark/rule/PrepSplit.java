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
import app.metatron.discovery.prep.parser.preparation.rule.Split;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.StringExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepSplit extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Split split = (Split) rule;
    Expression targetColExpr = split.getCol();
    Expression on = split.getOn();
    Integer limit = split.getLimit();
    assert limit != null;

    Expression quote = split.getQuote();
    assert quote == null : quote;

    Boolean ignoreCase = split.getIgnoreCase();
    assert ignoreCase == null : ignoreCase;

    assert on instanceof StringExpr : on;
    String strOn = ((StringExpr) on).getEscapedValue();

    List<String> targetColNames = getIdentifierList(targetColExpr);

    SparkUtil.createTempView(df, "temp");

    String sql = "SELECT ";
    for (int i = 0; i < df.columns().length; i++) {
      if (!targetColNames.contains(df.columns()[i])) {
        sql = String.format("%s%s, ", sql, df.columns()[i]);
        continue;
      }

      for (int j = 0; j <= limit; j++) {
        String newColName = String.format("split_%s%d", df.columns()[i], j + 1);
        sql = String.format("%ssplit_ex(%s, '%s', %d, %d) AS %s, ", sql, df.columns()[i], strOn, j, limit, newColName);
      }
    }
    sql = sql.substring(0, sql.length() - 2) + " FROM temp";
    return SparkUtil.getSession().sql(sql);
  }
}
