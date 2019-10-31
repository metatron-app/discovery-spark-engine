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
    // JSON -> Hive
    String dsUri = TestUtil.getResourcePath("json/crime.json");

    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: `Date`, `Location`, `Population_`, `Total_Crime`, `Violent_Crime`, `Property_Crime`, "
            + "`Murder_`, `Forcible_Rape_`, `Robbery_`, `Aggravated_Assault_`, `Burglary_`, `Larceny_Theft_`, "
            + "`Vehicle_Theft_` "
            + "to: 'dt', 'location', 'population', 'total_crime', 'violent_crime', 'property_crime', "
            + "'murder', 'forcible_rape', 'robbery', 'aggravated_assault', 'burglary', 'larceny_theft', "
            + "'vehicle_theft'");

    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_rename");

    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);

    // JSON -> Hive
    dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    ruleStrings.clear();
    ruleStrings.add("header");

    // CSV -> Hive
    snapshotInfo = new StagingDbSnapshotInfo("default", "test_sales");
    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);
  }

  @Test
  public void testKeep() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("keep row: `location` == 'NY'");

    TableInfo tableInfo = new TableInfo("default", "test_rename");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_keep");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }

  @Test
  public void testMove() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("move col: `robbery`, `aggravated_assault`, `burglary` before: `total_crime`");
    ruleStrings.add("move col: `total_crime` after: `vehicle_theft`");

    TableInfo tableInfo = new TableInfo("default", "test_rename");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_move");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }

  @Test
  public void testSort() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("sort order: `location`, `dt`");
    ruleStrings.add("sort order: `dt` type: 'desc'");

    TableInfo tableInfo = new TableInfo("default", "test_rename");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_sort");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }

  @Test
  public void testSplit() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("split col: `shipping_result`, `city` on: ' ' limit: 1");

    TableInfo tableInfo = new TableInfo("default", "test_sales");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_split");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }

  @Test
  public void testMerge() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("split col: `shipping_result`, `city` on: ' ' limit: 1");
    ruleStrings.add("merge col: split_city1, split_city2, split_shipping_result2 with: '-' as: 'result_per_cities'");
    ruleStrings.add("merge col: division, latitude, longitude with: '//' as: loc");

    TableInfo tableInfo = new TableInfo("default", "test_sales");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_merge");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }
}

