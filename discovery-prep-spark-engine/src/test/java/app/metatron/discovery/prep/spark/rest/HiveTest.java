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

