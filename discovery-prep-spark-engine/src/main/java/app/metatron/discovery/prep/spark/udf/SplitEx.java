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

package app.metatron.discovery.prep.spark.udf;

import org.apache.spark.sql.api.java.UDF4;

public class SplitEx implements UDF4<String, String, Integer, Integer, String> {

  @Override
  public String call(String coldata, String on, Integer nth, Integer limit) throws Exception {
    int begin = 0;

    // When on is " ":
    // "Phoenix" -> "Phoenix", NULL
    // "San Francisco" -> "San", "Francisco"
    // "New York City" -> "New", "York City"

    if (coldata == null) {
      return null;
    }

    // Skip as n
    for (int i = 0; i < nth; i++) {
      int idx = coldata.indexOf(on, begin);
      if (idx == -1) {
        return null;
      }
      begin = idx + on.length();
    }

    if (nth == limit) {
      return coldata.substring(begin);
    }

    // Now, it's the new column's value.
    int end = coldata.indexOf(on, begin);
    if (end == -1) {
      return coldata.substring(begin);
    }

    return coldata.substring(begin, end);
  }
}
