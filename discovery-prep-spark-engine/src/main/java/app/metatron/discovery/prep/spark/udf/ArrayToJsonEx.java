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

import app.metatron.discovery.prep.spark.util.GlobalObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.api.java.UDF2;
import scala.collection.Iterator;
import scala.collection.Seq;

public class ArrayToJsonEx implements UDF2<Seq<Object>, Seq<Object>, Object> {

  @Override
  public String call(Seq<Object> coldatas, Seq<Object> typeStrs) throws Exception {
    List<Object> list = new ArrayList();

    Iterator<Object> coldataIter = coldatas.iterator();
    Iterator<Object> typeStrIter = typeStrs.iterator();

    while (typeStrIter.hasNext()) {
      String typeStr = (String) typeStrIter.next();
      String coldata = (String) coldataIter.next();
      list.add(cast(coldata, typeStr));
    }
    return GlobalObjectMapper.getDefaultMapper().writeValueAsString(list);
  }

  private Object cast(Object obj, String typeStr) {
    if (obj == null) {
      return null;
    }

    switch (typeStr) {
      case "long":
        return Long.valueOf((String) obj);
      case "double":
        return Double.valueOf((String) obj);
      case "boolean":
        return Boolean.valueOf((String) obj);
      default:
        return obj;
    }
  }
}
