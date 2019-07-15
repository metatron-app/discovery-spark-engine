package app.metatron.discovery.prep.spark.rest;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;


public class JsonTest {

  @Test
  public void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    // JSON -> CSV
    String dsUri = TestUtil.getResourcePath("json/crime.json");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);

    // JSON -> JSON
    ssUri = "/tmp/dataprep/snapshots/crime.snapshot.json";
    TestUtil.testFileToJson(dsUri, ruleStrings, ssUri);
  }
}

