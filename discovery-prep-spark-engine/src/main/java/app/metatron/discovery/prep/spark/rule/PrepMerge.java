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

import app.metatron.discovery.prep.parser.preparation.rule.Merge;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepMerge extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException {
    Merge merge = (Merge) rule;

    Expression targetColExpr = merge.getCol();
    String with = stripQuotes(merge.getWith());
    String as = stripQuotes(merge.getAs());

    List<String> targetColNames = getIdentifierList(targetColExpr);

    SparkUtil.createTempView(df, "temp");

    String colNameList = "";
    int maxColno = -1;
    String maxColName = null;

    for (String colName : targetColNames) {
      colNameList += colName + ", ";
      if (getColno(df, colName) > maxColno) {
        maxColno = getColno(df, colName);
        maxColName = colName;
      }
    }
    colNameList = colNameList.substring(0, colNameList.length() - 2);
    assert maxColName != null;

    String sql = "SELECT ";
    for (int i = 0; i < df.columns().length; i++) {
      if (df.columns()[i].equals(maxColName)) {
        sql = String.format("%sconcat_ws('%s', %s) AS %s, ", sql, with, colNameList, as);
      }

      if (targetColNames.contains(df.columns()[i])) {
        continue;
      }

      sql = String.format("%s%s, ", sql, df.columns()[i]);
    }

    sql = sql.substring(0, sql.length() - 2) + " FROM temp";
    return SparkUtil.getSession().sql(sql);
  }

  private int getColno(Dataset<Row> df, String colName) {
    for (int i = 0; i < df.columns().length; i++) {
      if (df.columns()[i].equals(colName)) {
        return i;
      }
    }
    assert false : colName;
    return -1;
  }
}
