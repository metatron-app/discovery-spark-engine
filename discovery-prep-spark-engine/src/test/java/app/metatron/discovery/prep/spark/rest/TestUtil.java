package app.metatron.discovery.prep.spark.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.testng.Assert.assertEquals;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.httpclient.HttpStatus;


public class TestUtil {

  public static String BASE_URL = "http://localhost:8080";

  static String getResourcePath(String relPath, boolean fromHdfs) {
    if (fromHdfs) {
      throw new IllegalArgumentException("HDFS not supported yet");
    }
    URL url = TestUtil.class.getClassLoader().getResource(relPath);
    return (new File(url.getFile())).getAbsolutePath();
  }

  static String getResourcePath(String relPath) {
    return getResourcePath(relPath, false);
  }

  static Map<String, Object> buildPrepPropertiesInfo() {
    Map<String, Object> prepPropertiesInfo = new HashMap();

    prepPropertiesInfo.put("polaris.dataprep.spark.appName", "DiscoverySparkEngine");
    prepPropertiesInfo.put("polaris.dataprep.spark.master", "local");

    return prepPropertiesInfo;
  }

  static Map<String, Object> buildDatasetInfo(String absPath, String delimiter,
      List<String> ruleStrings) {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("storedUri", absPath);
    datasetInfo.put("delimiter", delimiter);
    datasetInfo.put("ruleStrings", ruleStrings);

    return datasetInfo;
  }

  static Map<String, Object> buildSnapshotInfo(String absPath, String format) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("storedUri", absPath);
    snapshotInfo.put("ssType", "LOCAL");
    snapshotInfo.put("format", format);

    return snapshotInfo;
  }

  static Map<String, Object> buildCallbackInfo() {
    Map<String, Object> callbackInfo = new HashMap();

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken",
        "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    return callbackInfo;
  }

  static void testCsvToCsv(String dsPath, List<String> ruleStrings, String ssPath) {
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

    assertEquals(response.path("result"), "SUCCEEDED");

    System.out.println(response.toString());
  }

}

