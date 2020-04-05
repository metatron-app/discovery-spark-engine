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
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.api.java.UDF2;

public class FromMapEx implements UDF2<String, String, String> {

  @Override
  public String call(String coldata, String key) throws Exception {
    Map<String, Object> map = getDefaultMapper().readValue(coldata, new TypeReference<Map<String, Object>>() {
    });

    Object obj = map.get(key);
    if (obj == null) {
      return null;
    }

    if (obj instanceof List || obj instanceof Map) {
      return getDefaultMapper().writeValueAsString(obj);
    }
    return obj.toString();
  }
}
