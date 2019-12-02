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

import static app.metatron.discovery.prep.spark.util.StringUtil.makeParsable;
import static app.metatron.discovery.prep.spark.util.StringUtil.modifyDuplicatedColName;

import app.metatron.discovery.prep.parser.preparation.rule.Header;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepHeader extends PrepRule {

  public static Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Dataset<Row> newDf = df;
    String newColName;

    Header header = (Header) rule;
    Integer rownum = header.getRownum();
    if (rownum == null) {
      rownum = 1;
    }

    newDf = newDf.withColumn("rowid", org.apache.spark.sql.functions.monotonically_increasing_id());

    String[] colNames = df.columns();
    Row row = newDf.filter("rowid = " + (rownum - 1)).head();
    newDf = newDf.filter("rowid != " + (rownum - 1));

    for (int i = 0; i < colNames.length; i++) {
      if (row.get(i) != null) {
        newColName = row.get(i).toString();
      } else {
        newColName = "column" + (i + 1);
      }
      newDf = newDf.withColumnRenamed(colNames[i], modifyDuplicatedColName(df, makeParsable(newColName)));
    }

    return newDf;
  }
}
