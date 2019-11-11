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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;


public class ExprTest {

  @Test
  public void testSet() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("set col: `sale_price` value: sale_price * 100");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDuplicate() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("derive value: `sale_price` as: `duplicated_sale_price`");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDerive() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("derive value: `sale_price` * 100 as: after_inflation");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceLiteral() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `business`, `base` on: 'Office' with: 'Desk'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExp() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `due` on: /\\\\d/ with: 'x'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExpGroup() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `due` on: /(\\\\d+).*(\\\\d+).*(\\\\d+).*/ with: '$1-$2-$3'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }
}
