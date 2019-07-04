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

  public static String BASE_URL = "http://localhost:8080";

  Map<String, Object> buildPrepPropertiesInfo() {
    Map<String, Object> prepPropertiesInfo = new HashMap();

    prepPropertiesInfo.put("polaris.dataprep.spark.appName", "DiscoverySparkEngine");
    prepPropertiesInfo.put("polaris.dataprep.spark.master", "local");

    return prepPropertiesInfo;
  }

  Map<String, Object> buildDatasetInfo(String absPath, String delimiter, List<String> ruleStrings) {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("storedUri", absPath);
    datasetInfo.put("delimiter", delimiter);
    datasetInfo.put("ruleStrings", ruleStrings);

    return datasetInfo;
  }

  Map<String, Object> buildSnapshotInfo(String absPath, String format) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("storedUri", absPath);
    snapshotInfo.put("ssType", "LOCAL");
    snapshotInfo.put("format", format);

    return snapshotInfo;
  }

  Map<String, Object> buildCallbackInfo() {
    Map<String, Object> callbackInfo = new HashMap();

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken",
        "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    return callbackInfo;
  }

  void testCsvToCsv(String dsPath, List<String> ruleStrings, String ssPath) {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(dsPath, ",", ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(ssPath, "CSV"));
    args.put("callbackInfo", buildCallbackInfo());

    Response response = given().contentType(ContentType.JSON)
        .accept(ContentType.JSON)
        .when()
        .content(args)
        .post(BASE_URL + "/run")
        .then()
        .log().all()
        .statusCode(HttpStatus.SC_OK)
        .extract()
        .response();

    assertEquals("SUCCEEDED", response.path("result"));

    System.out.println(response.toString());
  }

  @Test
  public void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testHeader() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Date` to: `DT`");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testWeirdHeader() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 5");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDrop() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("drop col: `Date`, `Location`");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testKeep() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("keep row: `Location` == 'NY'");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDelete() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header");
    ruleStrings.add("delete row: `Location` == 'NY' || `Location` == 'CA' || `Location` == 'US'");

    String dsPath = TestUtil.getResourcePath("csv/crime.csv");
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testLargeFile() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    String dsPath = "/tmp/dataprep/uploads/bigfile.csv";
    String ssPath = "/tmp/snapshots/bigfile.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDocker() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Date` to: `DT`");

    String dsPath = "/tmp/dataprep/uploads/crime.csv";
    String ssPath = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    testCsvToCsv(dsPath, ruleStrings, ssPath);
  }
}

