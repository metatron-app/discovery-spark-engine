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
}

