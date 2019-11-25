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

package app.metatron.discovery.prep.spark.rest;

import app.metatron.discovery.prep.spark.PrepTransformer;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.BeforeClass;
import org.junit.Test;


public class NestUnnestTest {

  @BeforeClass
  public static void setup() {
    SparkUtil.setAppName("DiscoverySparkEngine");
    SparkUtil.setMasterUri("local");
    SparkUtil.setWarehouseDir("hdfs://localhost:9000/user/hive/warehouse");
    SparkUtil.setMetastoreUris("thrift://localhost:9083");
  }

  @Test
  public void testNestUnnest() throws IOException, AnalysisException {
    Dataset<Row> df = TestUtil.readAsCsvFile(new String[][]{
            {"a", "b", "c"},
            {"3", "1", "2"},
            {"1", "1", "3"},
            {"3", "1", "4"},
            {"NULL", "NULL", "NULL"}
    });

    PrepTransformer transformer = new PrepTransformer();
    df = transformer.applyRule(df, "header rownum: 1", null);
    df = transformer.applyRule(df, "set col: a, b, c value: if($col == 'NULL', null, $col)", null);
    df = transformer.applyRule(df, "settype col: a, b type: long", null);
    df.show();

    List<Row> rows = df.collectAsList();
    TestUtil.assertRow(rows.get(0), new Object[]{3L, 1L, "2"});
    TestUtil.assertRow(rows.get(1), new Object[]{1L, 1L, "3"});
    TestUtil.assertRow(rows.get(2), new Object[]{3L, 1L, "4"});
    TestUtil.assertRow(rows.get(3), new Object[]{null, null, null});

    Dataset<Row> df2 = transformer.applyRule(df, "nest col: a, b, c into: 'map' as: d", null);
    df2.show();
    rows = df2.collectAsList();
    TestUtil.assertRow(rows.get(0), new Object[]{3L, 1L, "2", "{\"a\":3,\"b\":1,\"c\":\"2\"}"});
    TestUtil.assertRow(rows.get(1), new Object[]{1L, 1L, "3", "{\"a\":1,\"b\":1,\"c\":\"3\"}"});
    TestUtil.assertRow(rows.get(2), new Object[]{3L, 1L, "4", "{\"a\":3,\"b\":1,\"c\":\"4\"}"});
    TestUtil.assertRow(rows.get(3), new Object[]{null, null, null, "{}"});

    df2 = transformer.applyRule(df, "nest col: a, b, c into: 'array' as: d", null);
    df2.show();
    rows = df2.collectAsList();
    TestUtil.assertRow(rows.get(0), new Object[]{3L, 1L, "2", "[3,1,\"2\"]"});
    TestUtil.assertRow(rows.get(1), new Object[]{1L, 1L, "3", "[1,1,\"3\"]"});
    TestUtil.assertRow(rows.get(2), new Object[]{3L, 1L, "4", "[3,1,\"4\"]"});
    TestUtil.assertRow(rows.get(3), new Object[]{null, null, null, "[null,null,null]"});
  }
}
