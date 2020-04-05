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
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.ArrayExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.LongExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant.StringExpr;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.GlobalObjectMapper;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepUnnest extends PrepRule {

  private boolean contains(Expression expr, String key) {
    if (expr instanceof StringExpr) {
      String str = ((StringExpr) expr).getEscapedValue();
      return str.equals(key);
    } else if (expr instanceof ArrayExpr) {
      List<String> list = ((ArrayExpr) expr).getValue();
      for (String str : list) {
        if (key.equals(str.replaceAll("'", ""))) {
          return true;
        }
      }
      return false;
    }
    assert false : "Wrong expression type: " + expr;
    return false;
  }

  private boolean contains(Expression expr, int idx) {
    if (expr instanceof LongExpr) {
      return idx == (Long) ((LongExpr) expr).getValue();
    } else if (expr instanceof ArrayExpr) {
      List<Long> list = ((ArrayExpr) expr).getValue();
      for (long n : list) {
        if (idx == n) {
          return true;
        }
      }
      return false;
    }
    assert false : "Wrong expression type: " + expr;
    return false;
  }

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException, IOException {
    Unnest unnest = (Unnest) rule;
    String col = unnest.getCol();
    String into = unnest.getInto();
    Expression idx = unnest.getIdx();
    assert idx != null : "unnest idx shouldn't be null";

    List<String> elemKeys = new ArrayList();
    List<Integer> elemIdxs = new ArrayList();

    SparkUtil.createTempView(df, "temp");

    String sql = String.format("select %s from temp limit 1", col);
    String jsonStr = SparkUtil.getSession().sql(sql).collectAsList().get(0).get(0).toString();

    switch (into.toUpperCase()) {
      case "MAP":
        Map<String, Object> map = GlobalObjectMapper.getDefaultMapper().readValue(jsonStr, Map.class);
        for (String key : map.keySet()) {
          if (contains(idx, key)) {
            elemKeys.add(key);
          }
        }
        break;
      case "ARRAY":
        List<Object> list = GlobalObjectMapper.getDefaultMapper().readValue(jsonStr, List.class);
        for (int i = 0; i < list.size(); i++) {
          if (contains(idx, i)) {
            elemIdxs.add(i);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("PrepUnnest: Wrong type: " + into);
    }

    String outColStr = "";
    for (String colName : df.columns()) {
      outColStr = String.format("%s%s, ", outColStr, colName);
      if (colName.equals(col)) {
        switch (into.toUpperCase()) {
          case "MAP":
            outColStr = appendMapElems(df, outColStr, colName, elemKeys);
            break;
          case "ARRAY":
            outColStr = appendArrayElems(df, outColStr, colName, elemIdxs);
            break;
          default:
            assert false;
        }
      }
    }
    sql = String.format("SELECT %s FROM temp", outColStr.substring(0, outColStr.length() - 2));
    return getSession().sql(sql);
  }

  private String appendMapElems(Dataset<Row> df, String outColStr, String colName, List<String> elemKeys) {
    assert elemKeys != null;

    for (String key : elemKeys) {
      String newColName = key;
      String as = modifyIfDuplicated(df, newColName);
      outColStr = String.format("%sfrom_map_ex(%s, '%s') AS %s, ", outColStr, colName, key, as);
    }

    return outColStr;
  }

  private String appendArrayElems(Dataset<Row> df, String outColStr, String colName, List<Integer> elemIdxs) {
    assert elemIdxs != null;

    for (Integer idx : elemIdxs) {
      String newColName = colName + "_" + idx;
      String as = modifyIfDuplicated(df, newColName);
      outColStr = String.format("%sfrom_array_ex(%s, %d) AS %s, ", outColStr, colName, idx, as);
    }

    return outColStr;
  }

  private String modifyIfDuplicated(Dataset<Row> df, String colName) {
    String[] colNames = df.columns();

    if (!Arrays.asList(colNames).contains(colName)) {
      return colName;
    }

    // Increase until it's not duplicated.
    for (int i = 1; i < Integer.MAX_VALUE; i++) {
      String newColName = String.format("%s_%d", colName, i);
      if (!Arrays.asList(colNames).contains(colName)) {
        return newColName;
      }
    }

    assert false : colName;
    return null;
  }
}
