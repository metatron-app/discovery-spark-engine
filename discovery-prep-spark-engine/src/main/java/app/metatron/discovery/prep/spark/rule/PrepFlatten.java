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

import app.metatron.discovery.prep.parser.preparation.rule.Flatten;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepFlatten extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) throws AnalysisException, IOException {
    Flatten flatten = (Flatten) rule;
    String col = flatten.getCol();

    SparkUtil.createTempView(df, "temp");
    String outColStr = "";

    for (String colName : df.columns()) {
      if (colName.equals(col)) {
        outColStr = String.format("%sto_array_type_ex(%s) as %s, ", outColStr, colName, colName);
      } else {
        outColStr = String.format("%s%s, ", outColStr, colName);
      }
    }
    String sql = String.format("SELECT %s FROM temp", outColStr.substring(0, outColStr.length() - 2));
    Dataset<Row> tmpDf = getSession().sql(sql);

    SparkUtil.createTempView(tmpDf, "temp2");
    outColStr = "";

    for (String colName : tmpDf.columns()) {
      if (colName.equals(col)) {
        outColStr = String.format("%sexplode(%s) as %s, ", outColStr, colName, colName);
      } else {
        outColStr = String.format("%s%s, ", outColStr, colName);
      }
    }
    sql = String.format("SELECT %s FROM temp2", outColStr.substring(0, outColStr.length() - 2));
    return getSession().sql(sql);
  }
}
