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

import static app.metatron.discovery.prep.spark.util.GlobalObjectMapper.getDefaultMapper;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.api.java.UDF1;


public class ToArrayTypeEx implements UDF1<String, List<String>> {

  @Override
  public List<String> call(String coldata) throws Exception {
    if (coldata == null) {
      return null;
    }

    List<Object> objs = getDefaultMapper().readValue(coldata, new TypeReference<List<Object>>() {
    });

    List<String> strs = new ArrayList<>();
    for (Object obj : objs) {
      assert obj != null : strs;

      if (obj instanceof List || obj instanceof Map) {
        strs.add(getDefaultMapper().writeValueAsString(obj));
      } else {
        strs.add(obj.toString());
      }
    }

    return strs;
  }
}
