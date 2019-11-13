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
import java.util.Map;
import org.junit.Test;


public class BasicTest {

  @Test
  public void testPing() {

    TestUtil.testPing();
  }

  @Test
  public void testRename() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: column1 to: 'new_colname'");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri, TestUtil.getColCntByFirstLine(dsUri));
  }

  @Test
  public void testRenameHttpURLConnection() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: column1 to: 'new_colname'");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToCsvHttpURLConnection(dsUri, ruleStrings, ssUri, TestUtil.getColCntByFirstLine(dsUri));
  }

  @Test
  public void testHeader() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Date` to: 'DT'");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testWeirdHeader() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 5");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri, TestUtil.getColCntByFirstLine(dsUri));
  }

  @Test
  public void testDrop() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("drop col: `Date`, `Location`");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testKeep() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("keep row: `Location` == 'NY'");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDelete() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("delete row: `Location` == 'NY' || `Location` == 'CA' || `Location` == 'US'");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDynamicColcnt() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("merge col: column2, column3, column4, column5, column6 with: '_' as: 'all'");

    String dsUri = TestUtil.getResourcePath("csv/dynamic_colcnt.csv");
    String ssUri = "/tmp/dataprep/snapshots/dynamic_colcnt.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri, 6);
  }

//  @Test
  public void testUnion() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("union dataset2: 'ds2', 'ds3'");

    String dsUri1 = TestUtil.getResourcePath("csv/sales_2011_01.txt");
    String dsUri2 = TestUtil.getResourcePath("csv/sales_2011_02.txt");
    String dsUri3 = TestUtil.getResourcePath("csv/sales_2011_03.txt");

    int colCnt = TestUtil.getColCntByFirstLine(dsUri1);
    Map<String, Object> dsInfo1 = TestUtil.buildDatasetInfoWithDsId(dsUri1, ruleStrings, colCnt, "ds1");
    Map<String, Object> dsInfo2 = TestUtil.buildDatasetInfoWithDsId(dsUri2, new ArrayList(), colCnt, "ds2");
    Map<String, Object> dsInfo3 = TestUtil.buildDatasetInfoWithDsId(dsUri3, new ArrayList(), colCnt, "ds3");

    List<Map<String, Object>> dsList = new ArrayList();
    dsList.add(dsInfo2);
    dsList.add(dsInfo3);
    dsInfo1.put("upstreamDatasetInfos", dsList);

    String ssUri = "/tmp/dataprep/snapshots/union.csv";
    TestUtil.testFileToFileWithCustomDsInfo(dsInfo1, ssUri);
  }

  //  @Test
  public void testLargeFile() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    String dsUri = "/tmp/dataprep/uploads/bigfile.csv";
    String ssUri = "/tmp/dataprep/snapshots/bigfile.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }
}

