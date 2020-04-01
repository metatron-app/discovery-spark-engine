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
    ruleStrings.add("settype col: `price` type: Double");
    ruleStrings.add("set col: `price` value: price * 100");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDuplicate() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `price` type: Double");
    ruleStrings.add("derive value: `price` as: 'duplicated_sale_price'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDerive() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `price` type: Double");
    ruleStrings.add("derive value: `price` * 100 as: 'after_inflation'");

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
    String ssUri = "/tmp/dataprep/snapshots/replace_literal.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExp() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `due` on: /\\\\d/ with: 'x'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/replace_regex.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExpGroup() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `due` on: /(\\\\d+).*(\\\\d+).*(\\\\d+).*/ with: '$1-$2-$3'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/replace_regex_group.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExpOnUrl() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("extract col: `url` on: /utm_source=\\w+/ limit: 1");
    ruleStrings.add("replace col: `extract_url1` on: 'utm_source=' with: ''");

    String dsUri = TestUtil.getResourcePath("csv/utm_test.csv");
    String ssUri = "/tmp/dataprep/snapshots/replace_regex.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testExtractLiteral() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("extract col: `business`, `base` on: 'Office' limit: 2");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/extract_literal.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testExtractLiteralIgnoreCase() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("extract col: `city`, `state` on: 'sa' limit: 2 ignoreCase: true");
    ruleStrings.add("keep row: extract_city1 == 'Sa'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/extract_ignore_case.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testExtractRegExp() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("extract col: `due` on: /\\\\d+/ limit: 10");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/extract_regex.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testCountPatternLiteral() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("countpattern col: `business`, `desc` on: 'Office'");
    ruleStrings.add("keep row: countpattern_business_desc >= 2");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/countpattern_literal.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testCountPatternLiteralIgnoreCase() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("countpattern col: `city`, `desc` on: 'sa' ignoreCase: true");
    ruleStrings.add("keep row: countpattern_city_desc >= 2");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/countpattern_ignore_case.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testCountPatternRegExp() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("countpattern col: contract_date, `due` on: /\\\\d+/ quote: '-'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/countpattern_regexp.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }
}
