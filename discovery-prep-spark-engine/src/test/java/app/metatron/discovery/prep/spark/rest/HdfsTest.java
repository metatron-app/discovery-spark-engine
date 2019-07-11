package app.metatron.discovery.prep.spark.rest;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;


public class HdfsTest {

  @Test
  public void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    String dsUri = "hdfs://localhost:9000/dataprep/uploads/crime.csv";
    String ssUri = "hdfs://localhost:9000/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testHeader() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Date` to: `DT`");

    String dsUri = "hdfs://localhost:9000/dataprep/uploads/crime.csv";
    String ssUri = "hdfs://localhost:9000/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testWeirdHeader() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 5");

    String dsUri = "hdfs://localhost:9000/dataprep/uploads/crime.csv";
    String ssUri = "hdfs://localhost:9000/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDrop() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("drop col: `Date`, `Location`");

    String dsUri = "hdfs://localhost:9000/dataprep/uploads/crime.csv";
    String ssUri = "hdfs://localhost:9000/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testKeep() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("keep row: `Location` == 'NY'");

    String dsUri = "hdfs://localhost:9000/dataprep/uploads/crime.csv";
    String ssUri = "hdfs://localhost:9000/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDelete() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("delete row: `Location` == 'NY' || `Location` == 'CA' || `Location` == 'US'");

    String dsUri = "hdfs://localhost:9000/dataprep/uploads/crime.csv";
    String ssUri = "hdfs://localhost:9000/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsUri, ruleStrings, ssUri);
  }
}

