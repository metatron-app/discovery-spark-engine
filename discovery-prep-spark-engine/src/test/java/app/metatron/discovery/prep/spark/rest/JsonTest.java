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

public class JsonTest {

  @Test
  public void testRename() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: Date to: 'dt'");

    // JSON -> CSV
    String dsUri = TestUtil.getResourcePath("json/crime.json");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);

    // JSON -> JSON
    ssUri = "/tmp/dataprep/snapshots/crime.snapshot.json";
    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testUnnest1() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("settype col: b type: array");
    ruleStrings.add("settype col: c type: map");
    ruleStrings.add("unnest col: c into: map idx: 'c1', 'c3'");
    ruleStrings.add("unnest col: b into: array idx: 0, 1");
    ruleStrings.add("settype col: b_0 type: map");
    ruleStrings.add("unnest col: b_0 into: map idx: 'b1', 'b2'");

    String dsUri = TestUtil.getResourcePath("json/complex.json");
    String ssUri = "/tmp/dataprep/snapshots/unnest.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri, null);
  }

  @Test
  public void testUnnest2() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("settype col: `column2` type: map");
    ruleStrings.add("unnest col: `column1` into: MAP idx: 'A'");

    String dsUri = TestUtil.getResourcePath("csv/json_in_column.csv.txt");
    String ssUri = "/tmp/dataprep/snapshots/unnest.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri, TestUtil.getColCntByFirstLine(dsUri));
  }

  @Test
  public void testUnnestAll() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("settype col: `column2` type: map");
    ruleStrings.add("unnest col: `column2` into: MAP idx: ");   // UI sends like this. I don't like it.

    String dsUri = TestUtil.getResourcePath("csv/json_in_column.csv.txt");
    String ssUri = "/tmp/dataprep/snapshots/unnest_all.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri, 3);  // Need to implement escaping to use getColCntByFirstLine()
  }
}
