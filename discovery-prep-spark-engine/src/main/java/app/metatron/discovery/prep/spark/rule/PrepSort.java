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
import app.metatron.discovery.prep.parser.preparation.rule.Sort;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepSort extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Sort sort = (Sort) rule;
    Expression orderByColExpr = sort.getOrder();
    Expression typeExpr = sort.getType();

    List<String> orderByColNames = getIdentifierList(orderByColExpr);
    String desc = "";
    if (typeExpr != null) {
      assert typeExpr instanceof Constant.StringExpr : typeExpr;
      if (((Constant.StringExpr) typeExpr).getEscapedValue().equalsIgnoreCase("DESC")) {
        desc = " DESC";
      }
    }

    SparkUtil.createTempView(df, "temp");
    String sql = String.format("SELECT * FROM temp ORDER BY %s%s", toCommaSeparatedString(orderByColNames), desc);
    return SparkUtil.getSession().sql(sql);
  }

  private String toCommaSeparatedString(List<String> colNames) {
    String str = "";

    for (int i = 0; i < colNames.size(); i++) {
      str = String.format("%s`%s`, ", str, colNames.get(i));
    }

    return str.substring(0, str.length() - 2);
  }
}
