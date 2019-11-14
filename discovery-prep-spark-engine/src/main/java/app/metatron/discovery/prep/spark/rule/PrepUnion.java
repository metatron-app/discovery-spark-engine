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

import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepUnion extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, List<Dataset<Row>> dataset2) throws AnalysisException {
    dataset2.add(0, df);

    String sql = "";
    int i;
    for (i = 0; i < dataset2.size(); i++) {
      Dataset<Row> df2 = dataset2.get(i);
      SparkUtil.createTempView(df2, "temp" + i);
      sql = String.format("%sSELECT * FROM %s UNION ALL ", sql, "temp" + i);
    }
    sql = sql.substring(0, sql.length() - " UNION ALL".length());

    return SparkUtil.getSession().sql(sql);
  }
}
