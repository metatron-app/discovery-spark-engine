package app.metatron.discovery.prep.spark.rest;

import app.metatron.discovery.prep.spark.rest.TestUtil.StagingDbSnapshotInfo;
import app.metatron.discovery.prep.spark.rest.TestUtil.TableInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;


public class HiveTest {

  @Test
  public void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: Date to: dt");

    // JSON -> Hive
    String dsUri = TestUtil.getResourcePath("json/crime.json");

    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_rename");

    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);
  }

  @Test
  public void testKeep() {    // This test-case depends on testRename()
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("keep row: `Location` == 'NY'");

    TableInfo tableInfo = new TableInfo("default", "test_rename");
    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_keep");

    TestUtil.testHiveToHive(tableInfo, ruleStrings, snapshotInfo);
  }

  @Test
  public void testDocker() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: Date to: dt");

    // JSON -> Hive
    String dsUri = "file:///Users/jhkim/dataprep/uploads/crime.json";

    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo("default", "test_docker3");

    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);
  }
}

