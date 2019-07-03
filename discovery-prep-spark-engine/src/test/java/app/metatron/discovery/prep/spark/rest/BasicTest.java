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

  Map<String, Object> buildDatasetInfo(String relPath, String delimiter, List<String> ruleStrings) {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("storedUri", TestUtil.getResourcePath(relPath));
    datasetInfo.put("delimiter", delimiter);
    datasetInfo.put("ruleStrings", ruleStrings);

    return datasetInfo;
  }

  Map<String, Object> buildSnapshotInfo(String relPath, String format) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("storedUri", "/tmp/dataprep/snapshots/" + relPath);
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

  void testCsvToCsv(String dsRelPath, List<String> ruleStrings, String ssRelPath) {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(dsRelPath, ",", ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(ssRelPath, "CSV"));
    args.put("callbackInfo", buildCallbackInfo());

    Response response = given().contentType(ContentType.JSON)
        .accept(ContentType.JSON)
        .when()
        .content(args)
        .post(BASE_URL + "/run")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .extract()
        .response();

    assertEquals("OK", response.path("result"));

    System.out.println(response.toString());
  }

  @Test
  public void testRename() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    testCsvToCsv("csv/crime.csv", ruleStrings, "crime.snapshot.csv");
  }

}

