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

import org.apache.spark.sql.api.java.UDF2;

public class IsMismatchedEx implements UDF2<Object, String, Boolean> {

  @Override
  public Boolean call(Object coldata, String colType) throws Exception {
    if (coldata == null) {
      return false;
    }
    switch (colType) {
      case "LONG":
        if (coldata instanceof Long) {
          return false;   // matched
        }
        return false;
      case "DOUBLE":
        if (coldata instanceof Double) {
          Double d = (Double) coldata;
          if (d.isNaN() || d.isInfinite()) {
            return true;  // mismatched
          }
          return false;   // matched
        } else if (coldata instanceof Long) {
          return false;   // matched
        }
        return true;      // mismatched
      default:
    }
    return false;   // matched (Regard all cases that are not examined as matched)
  }
}
