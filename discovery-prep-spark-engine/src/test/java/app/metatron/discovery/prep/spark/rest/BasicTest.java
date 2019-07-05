package app.metatron.discovery.prep.spark.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Test;


public class BasicTest {

  @Test
  public void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testHeader() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Date` to: `DT`");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testWeirdHeader() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 5");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDrop() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("drop col: `Date`, `Location`");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testKeep() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("keep row: `Location` == 'NY'");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDelete() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("delete row: `Location` == 'NY' || `Location` == 'CA' || `Location` == 'US'");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  //  @Test
  public void testLargeFile() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    String dsPath = "/tmp/dataprep/uploads/bigfile.csv";
    String ssPath = "/tmp/snapshots/bigfile.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  //  @Test
  public void testDocker() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Date` to: `DT`");

    String dsPath = "/tmp/dataprep/uploads/crime.csv";
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }
}

