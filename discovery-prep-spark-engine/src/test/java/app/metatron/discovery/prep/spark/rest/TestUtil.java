package app.metatron.discovery.prep.spark.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.httpclient.HttpStatus;


public class TestUtil {

  public static String BASE_URL = "http://localhost:5300";

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

  static void testPing() {
    Response response = given().contentType(ContentType.JSON)
        .accept(ContentType.JSON)
        .when()
        .post(BASE_URL + "/ping")
        .then()
        .log().all()
        .statusCode(HttpStatus.SC_OK)
        .extract()
        .response();

    assertEquals(response.path("result"), "SUCCEEDED");

    System.out.println(response.toString());
  }

  static Map<String, Object> buildPrepPropertiesInfo() {
    Map<String, Object> prepPropertiesInfo = new HashMap();

    String uri = "thrift://localhost:9083";
    String warehouseDir = "hdfs://localhost:9000/user/hive/warehouse2";

    prepPropertiesInfo.put("polaris.dataprep.etl.spark.appName", "DiscoverySparkEngine");
    prepPropertiesInfo.put("polaris.dataprep.etl.spark.master", "local");
    prepPropertiesInfo.put("polaris.storage.stagedb.metastore.uri", uri);
    prepPropertiesInfo.put("polaris.dataprep.spark.warehouseDir", warehouseDir);

    return prepPropertiesInfo;
  }

  static Map<String, Object> buildDatasetInfo(String ssUri, String delimiter,
      List<String> ruleStrings) {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("importType", "URI");
    datasetInfo.put("storedUri", ssUri);
    datasetInfo.put("delimiter", delimiter);
    datasetInfo.put("ruleStrings", ruleStrings);

    return datasetInfo;
  }

  static Map<String, Object> buildDatasetInfo(TableInfo tableInfo, List<String> ruleStrings) {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("importType", "STAGING_DB");
    datasetInfo.put("dbName", tableInfo.dbName);
    datasetInfo.put("tblName", tableInfo.tblName);
    datasetInfo.put("ruleStrings", ruleStrings);

    return datasetInfo;
  }

  static Map<String, Object> buildSnapshotInfo(String absPath, String format) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("storedUri", absPath);
    snapshotInfo.put("ssType", "URI");
    snapshotInfo.put("format", format);

    return snapshotInfo;
  }

  static Map<String, Object> buildSnapshotInfo(StagingDbSnapshotInfo hiveSnapshotInfo) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("ssType", "STAGING_DB");
    snapshotInfo.put("dbName", hiveSnapshotInfo.tableInfo.dbName);
    snapshotInfo.put("tblName", hiveSnapshotInfo.tableInfo.tblName);

    return snapshotInfo;
  }

  static Map<String, Object> buildCallbackInfo() {
    Map<String, Object> callbackInfo = new HashMap();

    callbackInfo.put("port", 8180);
    callbackInfo.put("oauthToken",
        "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1MzYyNTY4NTEsInVzZXJfbmFtZSI6InBvbGFyaXMiLCJhdXRob3JpdGllcyI6WyJQRVJNX1NZU1RFTV9NQU5BR0VfU0hBUkVEX1dPUktTUEFDRSIsIl9fU0hBUkVEX1VTRVIiLCJQRVJNX1NZU1RFTV9NQU5BR0VfREFUQVNPVVJDRSIsIlBFUk1fU1lTVEVNX01BTkFHRV9QUklWQVRFX1dPUktTUEFDRSIsIlBFUk1fU1lTVEVNX1ZJRVdfV09SS1NQQUNFIiwiX19EQVRBX01BTkFHRVIiLCJfX1BSSVZBVEVfVVNFUiJdLCJqdGkiOiI3MzYxZjU2MS00MjVmLTQzM2ItOGYxZC01Y2RmOTlhM2RkMWIiLCJjbGllbnRfaWQiOiJwb2xhcmlzX2NsaWVudCIsInNjb3BlIjpbIndyaXRlIl19.iig9SBPrNUXoHp2wxGgZczfwt71fu3RBuRc14HxYxvg");

    return callbackInfo;
  }

  static void testFileToCsv(String dsUri, List<String> ruleStrings, String ssUri) {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(dsUri, ",", ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(ssUri, "CSV"));
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

  static void testFileToCsvHttpURLConnection(String dsUri, List<String> ruleStrings, String ssUri)
      throws IOException {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(dsUri, ",", ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(ssUri, "CSV"));
    args.put("callbackInfo", buildCallbackInfo());

    URL url = new URL(BASE_URL + "/run");
    HttpURLConnection con = (HttpURLConnection) url.openConnection();

    con.setRequestMethod("POST");
    con.setRequestProperty("Content-Type", "application/json; utf-8");
    con.setRequestProperty("Accept", "application/json");
    con.setDoOutput(true);

    ObjectMapper mapper = new ObjectMapper();
    String jsonArgs = mapper.writeValueAsString(args);

    try (OutputStream os = con.getOutputStream()) {
      byte[] input = jsonArgs.getBytes("utf-8");
      os.write(input, 0, input.length);
    }

    InputStreamReader reader = new InputStreamReader(con.getInputStream(), "utf-8");
    try (BufferedReader br = new BufferedReader(reader)) {
      StringBuilder response = new StringBuilder();
      String responseLine = null;

      while (true) {
        responseLine = br.readLine();
        if (responseLine == null) {
          break;
        }
        response.append(responseLine.trim());
      }
      System.out.println(response.toString());
    }
  }

  static void testFileToJson(String dsUri, List<String> ruleStrings, String ssUri) {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(dsUri, ",", ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(ssUri, "JSON"));
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

  public static class TableInfo {

    public String dbName;
    public String tblName;

    public TableInfo() {
    }

    public TableInfo(String dbName, String tblName) {
      this.dbName = dbName;
      this.tblName = tblName;
    }
  }

  public static class StagingDbSnapshotInfo {

    TableInfo tableInfo;

    // TO BE ADDED (like compression, partition, append mode, ...)

    public StagingDbSnapshotInfo() {
    }

    public StagingDbSnapshotInfo(String dbName, String tblName) {
      tableInfo = new TableInfo(dbName, tblName);
    }
  }

  static void testFileToHive(String dsUri, List<String> ruleStrings,
      StagingDbSnapshotInfo stagingDbSnapshotInfo) {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(dsUri, ",", ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(stagingDbSnapshotInfo));
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

  static void testHiveToHive(TableInfo tableInfo, List<String> ruleStrings,
      StagingDbSnapshotInfo stagingDbSnapshotInfo) {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("datasetInfo", buildDatasetInfo(tableInfo, ruleStrings));
    args.put("snapshotInfo", buildSnapshotInfo(stagingDbSnapshotInfo));
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

