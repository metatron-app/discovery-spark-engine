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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.api.java.UDF4;

public class RegexpExtractEx implements UDF4<String, String, Integer, String, String> {

  @Override
  public String call(String coldata, String patternStr, Integer nth, String quoteStr) throws Exception {
    if (coldata == null) {
      return null;
    }

    if (org.apache.commons.lang3.StringUtils.countMatches(coldata, quoteStr) % 2 == 1) {
      coldata = coldata.substring(0, coldata.lastIndexOf(quoteStr));
    }

    Pattern pattern = Pattern.compile(patternStr);
    Matcher m = pattern.matcher(coldata);
    while (m.find()) {
      if (nth-- == 0) {
        return m.group();
      }
    }
    return null;
  }
}
