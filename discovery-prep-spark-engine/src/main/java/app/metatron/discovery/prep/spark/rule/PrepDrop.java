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

import app.metatron.discovery.prep.parser.preparation.rule.Drop;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepDrop extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Dataset<Row> newDf = df;

    Drop drop = (Drop) rule;
    Expression col = drop.getCol();

    List<String> colNames = getIdentifierList(col);

    for (int i = 0; i < colNames.size(); i++) {
      newDf = newDf.drop(colNames.get(i));
    }

    return newDf;
  }
}
