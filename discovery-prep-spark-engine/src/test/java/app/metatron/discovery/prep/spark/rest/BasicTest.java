package app.metatron.discovery.prep.spark.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
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

  Map<String, Object> buildJsonPrepPropertiesInfo() throws JsonProcessingException {
    Map<String, Object> prepPropertiesInfo = new HashMap();

    prepPropertiesInfo.put("polaris.dataprep.etl.limitRows", 1000000);
    prepPropertiesInfo.put("polaris.dataprep.etl.cores", 0);
    prepPropertiesInfo.put("polaris.dataprep.etl.timeout", 86400);

    prepPropertiesInfo.put("polaris.dataprep.spark.appName", "DiscoverySparkEngine");
    prepPropertiesInfo.put("polaris.dataprep.spark.master", "local");

    return prepPropertiesInfo;
  }

  Map<String, Object> buildJsonDatasetInfo(List<String> ruleStrings)
      throws JsonProcessingException {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("storedUri", TestUtil.getResourcePath("csv/crime.csv"));
    datasetInfo.put("delimiter", ",");
    datasetInfo.put("ruleStrings", ruleStrings);

    return datasetInfo;
  }

  Map<String, Object> buildJsonSnapshotInfo() throws JsonProcessingException {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("storedUri", "/tmp/dataprep/snapshots/crime.snapshot.csv");
    snapshotInfo.put("ssType", "HDFS");
    snapshotInfo.put("engine", "SPARK");
    snapshotInfo.put("format", "CSV");
    snapshotInfo.put("compression", "NONE");
    snapshotInfo.put("ssName", "crime_20180913_053230");
    snapshotInfo.put("ssId", "6e3eec52-fc60-4309-b0de-a53f93e08ce9");

    return snapshotInfo;
  }

  Map<String, Object> buildJsonCallbackInfo() throws JsonProcessingException {
    Map<String, Object> callbackInfo = new HashMap();

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken",
        "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    return callbackInfo;
  }

  void testCrime(List<String> ruleStrings) throws JsonProcessingException {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildJsonPrepPropertiesInfo());
    args.put("datasetInfo", buildJsonDatasetInfo(ruleStrings));
    args.put("snapshotInfo", buildJsonSnapshotInfo());
    args.put("callbackInfo", buildJsonCallbackInfo());

    Response response = given()
        .contentType(ContentType.JSON)
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
  public void testRename() throws JsonProcessingException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("rename col: _c0 to: new_colname");

    testCrime(ruleStrings);
  }

}

