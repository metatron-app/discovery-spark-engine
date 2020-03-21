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

import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.Unnest;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.GlobalObjectMapper;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepUnnest extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException, IOException {
    Unnest unnest = (Unnest) rule;
    String col = unnest.getCol();
    String into = unnest.getInto();
    Expression idx = unnest.getIdx();

    List<String> elemKeys = new ArrayList();
    List<Integer> elemIdxs = new ArrayList();

    SparkUtil.createTempView(df, "temp");

    if (idx == null) {
      String sql = String.format("select %s from temp limit 1", col);
      String jsonStr = SparkUtil.getSession().sql(sql).collectAsList().get(0).get(0).toString();

      switch (into.toUpperCase()) {
        case "MAP":
          Map<String, Object> map = GlobalObjectMapper.getDefaultMapper().readValue(jsonStr, Map.class);
          for (String key : map.keySet()) {
            elemKeys.add(key);
          }
          break;
        case "ARRAY":
          List<Object> list = GlobalObjectMapper.getDefaultMapper().readValue(jsonStr, List.class);
          for (int i = 0; i < list.size(); i++) {
            elemIdxs.add(i);
          }
          break;
        default:
          throw new IllegalArgumentException("PrepUnnest: Wrong type: " + into);
      }
    }

    String outColStr = "";
    for (String colName : df.columns()) {
      outColStr = String.format("%s%s, ", outColStr, colName);
      if (colName.equals(col)) {
        outColStr = appendNewColumns(df, outColStr, colName, elemKeys, null);
      }
    }
    String sql = String.format("SELECT %s FROM temp", outColStr.substring(0, outColStr.length() - 2));
    return getSession().sql(sql);
  }

  private String appendNewColumns(Dataset<Row> df, String outColStr, String colName, List<String> elemKeys,
          List<Integer> elemIdxs) {
    if (elemKeys != null) {
      for (String key : elemKeys) {
        String as = key;
        while (contains(df.columns(), as)) {
          as += "_1";
        }
        outColStr = String.format("%sget_json_object(%s, '$.%s') AS %s, ", outColStr, colName, key, as);
      }
    }
    return outColStr;
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
