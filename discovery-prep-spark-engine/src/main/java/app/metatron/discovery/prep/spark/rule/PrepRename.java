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

import app.metatron.discovery.prep.parser.preparation.rule.Rename;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepRename extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Dataset<Row> newDf = df;

    Rename rename = (Rename) rule;
    Expression col = rename.getCol();
    Expression to = rename.getTo();

    List<String> colNames = getIdentifierList(col);
    List<String> toNames = getIdentifierList(to);

    for (int i = 0; i < colNames.size(); i++) {
      newDf = newDf.withColumnRenamed(colNames.get(i), toNames.get(i));
    }

    return newDf;
  }
}
