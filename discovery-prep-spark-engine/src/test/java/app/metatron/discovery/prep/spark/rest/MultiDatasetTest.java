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


public class MultiDatasetTest {

  @Test
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

  @Test
  public void testJoinInner() throws IOException {
    List<String> ruleStringsForA = new ArrayList();
    List<String> ruleStringsForB = new ArrayList();

    String dsUriA = TestUtil.getResourcePath("csv/A.txt");
    String dsUriB = TestUtil.getResourcePath("csv/B.txt");

    ruleStringsForA.add("header");
    ruleStringsForA.add("join leftSelectCol: id, name rightSelectCol: id, occupation condition: id=id"
            + " joinType: 'outer' dataset2: 'dsB'");

    ruleStringsForB.add("header");

    int colCnt = TestUtil.getColCntByFirstLine(dsUriA);
    Map<String, Object> dsInfoA = TestUtil.buildDatasetInfoWithDsId(dsUriA, ruleStringsForA, colCnt, "dsA");
    Map<String, Object> dsInfoB = TestUtil.buildDatasetInfoWithDsId(dsUriB, ruleStringsForB, colCnt, "dsB");

    List<Map<String, Object>> dsList = new ArrayList();
    dsList.add(dsInfoB);
    dsInfoA.put("upstreamDatasetInfos", dsList);

    String ssUri = "/tmp/dataprep/snapshots/join1.csv";
    TestUtil.testFileToFileWithCustomDsInfo(dsInfoA, ssUri);
  }

  @Test
  public void testJoinOuterAndMultiCol() throws IOException {
    List<String> ruleStringsForA = new ArrayList();
    List<String> ruleStringsForB = new ArrayList();

    String dsUriA = TestUtil.getResourcePath("csv/A.txt");
    String dsUriB = TestUtil.getResourcePath("csv/B.txt");

    ruleStringsForA.add("header");
    ruleStringsForA.add("derive value: id as id2");
    ruleStringsForA.add("join leftSelectCol: id, id2, `name` rightSelectCol: occupation condition: id=id && id2=id2"
            + " joinType: 'outer' dataset2: 'dsB'");

    ruleStringsForB.add("header");
    ruleStringsForB.add("derive value: id as id2");

    int colCnt = TestUtil.getColCntByFirstLine(dsUriA);
    Map<String, Object> dsInfoA = TestUtil.buildDatasetInfoWithDsId(dsUriA, ruleStringsForA, colCnt, "dsA");
    Map<String, Object> dsInfoB = TestUtil.buildDatasetInfoWithDsId(dsUriB, ruleStringsForB, colCnt, "dsB");

    List<Map<String, Object>> dsList = new ArrayList();
    dsList.add(dsInfoB);
    dsInfoA.put("upstreamDatasetInfos", dsList);

    String ssUri = "/tmp/dataprep/snapshots/join2.csv";
    TestUtil.testFileToFileWithCustomDsInfo(dsInfoA, ssUri);
  }
}

