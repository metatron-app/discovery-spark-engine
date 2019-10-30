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

import app.metatron.discovery.prep.parser.preparation.rule.Move;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepMove extends PrepRule {

  public Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Move drop = (Move) rule;
    Expression col = drop.getCol();
    String after = drop.getAfter();
    String before = drop.getBefore();

    List<String> targets = getIdentifierList(col);

    // Skip checking if they're consecutive: it must be checked in metatron discovery in sampling stage.

    int fromColno = -1;
    int toColno = -1;

    for (int i = 0; i < df.columns().length; i++) {
      if (df.columns()[i].equals(targets.get(0))) {
        fromColno = i;
        break;
      }
    }
    assert fromColno >= 0 : targets;

    for (int i = 0; i < df.columns().length; i++) {
      if (df.columns()[i].equals(after)) {
        toColno = i + 1;
        break;
      }

      if (df.columns()[i].equals(before)) {
        toColno = i;
        break;
      }
    }
    assert toColno >= 0 : targets;

    SparkUtil.createTempView(df, "temp");
    String sql = String.format("SELECT %s FROM temp",
            toCommaSeparatedString(getReorderedColNames(df, targets, fromColno, toColno)));

    return SparkUtil.getSession().sql(sql);
  }

  private String toCommaSeparatedString(String[] colNames) {
    String str = "";

    for (int i = 0; i < colNames.length; i++) {
      str = String.format("%s`%s`, ", str, colNames[i]);
    }

    return str.substring(0, str.length() - 2);
  }

  private String[] getReorderedColNames(Dataset<Row> df, List<String> targets, int fromColno, int toColno) {
    String[] newColNames = new String[df.columns().length];
    int i = 0;

    if (fromColno < toColno) {
      // A B C D E F G H I J K L
      //       - F -
      //                   T
      // A B C G H I D E F J K L

      // A B C
      for (i = 0; i < fromColno; i++) {
        newColNames[i] = df.columns()[i];
      }

      // G H I
      for (/* NOP */; i < toColno - targets.size(); i++) {
        newColNames[i] = df.columns()[i + targets.size()];
      }

      // D E F
      for (int j = 0; j < targets.size(); j++) {
        newColNames[i++] = targets.get(j);
      }

      // J K L
      for (/* NOP */; i < df.columns().length; i++) {
        newColNames[i] = df.columns()[i];
      }
    } else {
      // A B C D E F G H I J K L
      //             - F -
      //   T
      // A G H I B C D E F J K L

      // A
      for (i = 0; i < toColno; i++) {
        newColNames[i] = df.columns()[i];
      }

      // G H I
      for (int j = 0; j < targets.size(); j++) {
        newColNames[i++] = targets.get(j);
      }

      // B C D E F
      for (/* NOP */; i < fromColno + targets.size(); i++) {
        newColNames[i] = df.columns()[i - targets.size()];
      }

      // J K L
      for (/* NOP */; i < df.columns().length; i++) {
        newColNames[i] = df.columns()[i];
      }
    }

    return newColNames;
  }
}
