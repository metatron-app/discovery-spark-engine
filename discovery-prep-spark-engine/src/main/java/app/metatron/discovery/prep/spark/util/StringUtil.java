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

package app.metatron.discovery.prep.spark.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class StringUtil {

  public static String makeParsable(String colName) {
    if (colName.matches("^\'.+\'")) {
      colName = colName.substring(1, colName.length() - 1);
    }

    return colName.replaceAll("[\\p{Punct}\\p{IsPunctuation}]", "_");
  }

  private static void assertParsable(String colName) {
    assert makeParsable(colName).equals(colName) : colName;
  }

  public static String modifyDuplicatedColName(Dataset<Row> df, String colName) {
    return modifyDuplicatedColName(df.columns(), colName);
  }

  public static String modifyDuplicatedColName(String[] curColNames, String colName) {
    assertParsable(colName);

    if (!colsContains(curColNames, colName)) {
      return colName;
    }

    // 숫자를 1씩 늘려가면서 중복 체크
    for (int i = 1; i < Integer.MAX_VALUE; i++) {
      String newColName = String.format("%s_%d", colName, i);
      if (!colsContains(curColNames, newColName)) {
        return newColName;
      }
    }

    assert false : colName;
    return null;
  }

  private static boolean colsContains(String[] curColNames, String colName) {
    for (int i = 0; i < curColNames.length; i++) {
      if (curColNames[i].equalsIgnoreCase(colName)) {
        return true;
      }
    }
    return false;
  }
}
