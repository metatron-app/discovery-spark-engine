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

import app.metatron.discovery.prep.spark.rest.TestUtil.StagingDbSnapshotInfo;
import app.metatron.discovery.prep.spark.rest.TestUtil.TableInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;


public class HiveTest {

  @BeforeClass    // All test-cases in this class depend on table "test_rename"
  public static void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: Date to: dt");

    // JSON -> Hive
    String dsUri = TestUtil.getResourcePath("json/crime.json");

    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_rename");

    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);
  }

  @Test
  public void testKeep() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("keep row: `Location` == 'NY'");

    TableInfo tableInfo = new TableInfo("default", "test_rename");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_keep");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }
}

