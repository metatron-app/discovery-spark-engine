package app.metatron.discovery.prep.spark.rest;

import app.metatron.discovery.prep.spark.rest.TestUtil.StagingDbSnapshotInfo;
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

    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo();
    snapshotInfo.dbName = "default";
    snapshotInfo.tblName = "test_rename";

    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);
  }

  @Test
  public void testDocker() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: Date to: dt");

    // JSON -> Hive
    String dsUri = "/Users/jhkim/dataprep/uploads/crime.json";

    StagingDbSnapshotInfo snapshotInfo = new StagingDbSnapshotInfo();
    snapshotInfo.dbName = "default";
    snapshotInfo.tblName = "test_docker";

    TestUtil.testFileToHive(dsUri, ruleStrings, snapshotInfo);
  }
}

