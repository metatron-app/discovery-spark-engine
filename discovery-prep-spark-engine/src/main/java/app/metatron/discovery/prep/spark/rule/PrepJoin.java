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

import app.metatron.discovery.prep.parser.preparation.rule.Join;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.BinAndExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expr.BinAsExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Identifier.IdentifierExpr;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrepJoin extends PrepRule {

  private static Logger LOGGER = LoggerFactory.getLogger(PrepJoin.class);

  public Dataset<Row> transform(Dataset<Row> df, Rule rule, List<Dataset<Row>> dataset2) throws AnalysisException {
    assert dataset2.size() == 1 : dataset2.size();
    Dataset<Row> df2 = dataset2.get(0);

    SparkUtil.createTempView(df, "temp_left");
    SparkUtil.createTempView(df2, "temp_right");

    Join join = (Join) rule;

    Expression leftSelectCol = join.getLeftSelectCol();
    Expression rightSelectCol = join.getRightSelectCol();
    Expression condition = join.getCondition();
    String joinType = join.getJoinType();

    List<String> lSelectCols = getIdentifierList(leftSelectCol);
    List<String> rSelectCols = getIdentifierList(rightSelectCol);

    List<String> outCols = new ArrayList();
    outCols.addAll(lSelectCols);
    for (int i = 0; i < rSelectCols.size(); i++) {
      String colName = rSelectCols.get(i);
      if (lSelectCols.contains(colName)) {
        outCols.add("r_" + colName);
      } else {
        outCols.add(colName);
      }
    }

    // rename duplicated column names
    for (int i = 1; i < outCols.size(); i++) {
      String colName = outCols.get(i);
      while (outCols.subList(0, i).contains(colName)) {
        colName = colName + "_1";
      }
      outCols.set(i, colName);
    }

    String condStr = getCondStr(condition);

    String sql = "SELECT ";
    for (int i = 0; i < lSelectCols.size(); i++) {
      sql = String.format("%sa.%s AS %s, ", sql, lSelectCols.get(i), outCols.get(i));
    }
    for (int i = 0; i < rSelectCols.size(); i++) {
      sql = String.format("%sb.%s AS %s, ", sql, rSelectCols.get(i), outCols.get(i + lSelectCols.size()));
    }
    sql = sql.substring(0, sql.length() - 2);
    sql = String.format("%s FROM temp_left AS a %s temp_right AS b ON %s ", sql, getJoinStr(joinType), condStr);

    return SparkUtil.getSession().sql(sql);
  }

  private String getCondStr(Expression cond) {
    if (cond instanceof BinAndExpr) {
      BinAndExpr andExpr = (BinAndExpr) cond;
      return getCondStr(andExpr.getLeft()) + " AND " + getCondStr(andExpr.getRight());
    }

    if (cond instanceof BinAsExpr) {
      BinAsExpr asExpr = (BinAsExpr) cond;
      String lColName = ((IdentifierExpr) asExpr.getLeft()).getValue();
      String rColName = ((IdentifierExpr) asExpr.getRight()).getValue();
      return String.format("a.%s = b.%s", lColName, rColName);
    }

    String msg = "Wrong join condition expression: " + cond;
    LOGGER.error(msg);
    throw new IllegalArgumentException(msg);
  }

  private String getJoinStr(String joinType) {
    joinType = joinType.substring(1, joinType.length() - 1);  // TODO: change joinType into StringExpr

    switch (joinType.toUpperCase()) {
      case "INNER":
        return "JOIN";
      case "LEFT":
        return "LEFT OUTER JOIN";
      case "RIGHT":
        return "RIGHT OUTER JOIN";
      case "OUTER":
        return "FULL OUTER JOIN";
      default:
        String msg = "Wrong join type: " + joinType;
        LOGGER.error(msg);
        throw new IllegalArgumentException(msg);
    }
  }
}
