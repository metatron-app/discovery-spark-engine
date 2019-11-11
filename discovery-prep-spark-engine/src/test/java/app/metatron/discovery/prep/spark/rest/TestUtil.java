/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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


  static Map<String, Object> buildPrepPropertiesInfo() {
    Map<String, Object> prepPropertiesInfo = new HashMap();

    String uri = "thrift://localhost:9083";
    String warehouseDir = "hdfs://localhost:9000/user/hive/warehouse";

    prepPropertiesInfo.put("polaris.storage.stagedb.metastore.uri", uri);
    prepPropertiesInfo.put("polaris.dataprep.etl.spark.appName", "DiscoverySparkEngine");
    prepPropertiesInfo.put("polaris.dataprep.etl.spark.master", "local");
    prepPropertiesInfo.put("polaris.dataprep.etl.spark.warehouseDir", warehouseDir);

    prepPropertiesInfo.put("polaris.dataprep.etl.limitRows", 30000);

    return prepPropertiesInfo;
  }

  static Map<String, Object> buildDatasetInfo(String ssUri, String delimiter, List<String> ruleStrings,
          Integer manualColumnCount) {
    Map<String, Object> datasetInfo = new HashMap();

    datasetInfo.put("importType", "URI");
    datasetInfo.put("storedUri", ssUri);
    datasetInfo.put("delimiter", delimiter);
    datasetInfo.put("ruleStrings", ruleStrings);
    datasetInfo.put("manualColumnCount", manualColumnCount);

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

  static Map<String, Object> buildSnapshotInfo(String absPath) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("ssId", "TestUtil");
    snapshotInfo.put("ssType", "URI");
    snapshotInfo.put("storedUri", absPath);

    return snapshotInfo;
  }

  static Map<String, Object> buildSnapshotInfo(StagingDbSnapshotInfo hiveSnapshotInfo) {
    Map<String, Object> snapshotInfo = new HashMap();

    snapshotInfo.put("ssId", "TestUtil");
    snapshotInfo.put("ssType", "STAGING_DB");
    snapshotInfo.put("dbName", hiveSnapshotInfo.tableInfo.dbName);
    snapshotInfo.put("tblName", hiveSnapshotInfo.tableInfo.tblName);

    return snapshotInfo;
  }

  static Map<String, Object> buildCallbackInfo() {
    Map<String, Object> callbackInfo = new HashMap();

    callbackInfo.put("port", "0");
    callbackInfo.put("oauthToken", "fake");

    return callbackInfo;
  }

  static private void testToSucceed(List<String> ruleStrings, String dsUri, String ssUri, TableInfo tableInfo,
          StagingDbSnapshotInfo stagingDbSnapshotInfo, boolean useRestAssure, Integer manualColumnCount)
          throws IOException {
    Map<String, Object> args = new HashMap();

    args.put("prepProperties", buildPrepPropertiesInfo());
    args.put("callbackInfo", buildCallbackInfo());

    if (dsUri != null) {
      args.put("datasetInfo", buildDatasetInfo(dsUri, ",", ruleStrings, manualColumnCount));
    } else {
      assert tableInfo != null;
      args.put("datasetInfo", buildDatasetInfo(tableInfo, ruleStrings));
    }

    if (ssUri != null) {
      args.put("snapshotInfo", buildSnapshotInfo(ssUri));
    } else {
      assert stagingDbSnapshotInfo != null;
      args.put("snapshotInfo", buildSnapshotInfo(stagingDbSnapshotInfo));
    }

    if (useRestAssure) {
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
      return;
    }

    // This is a test for real use of the API of discovery-spark-engine,
    // because the actual call method is a common HTTP POST request, not REST Assured test suite.
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
      String responseLine;

      while (true) {
        responseLine = br.readLine();
        if (responseLine == null) {
          break;
        }
        response.append(responseLine.trim());
      }

      System.out.println(response.toString());

      Map<String, Object> responseMap = mapper.readValue(response.toString(), HashMap.class);
      assertEquals((String) responseMap.get("result"), "SUCCEEDED");
    }
  }

  static void testFileToFile(String dsUri, List<String> ruleStrings, String ssUri) throws IOException {
    testToSucceed(ruleStrings, dsUri, ssUri, null, null, true, null);
  }

  static void testFileToFile(String dsUri, List<String> ruleStrings, String ssUri, Integer manualColumnCount)
          throws IOException {
    testToSucceed(ruleStrings, dsUri, ssUri, null, null, true, manualColumnCount);
  }

  // This is a test for real use of the API of discovery-spark-engine,
  // because the actual call method is a common HTTP POST request, not REST Assured test suite.
  static void testFileToCsvHttpURLConnection(String dsUri, List<String> ruleStrings, String ssUri) throws IOException {
    testToSucceed(ruleStrings, dsUri, ssUri, null, null, false, null);
  }

  static void testFileToHive(String dsUri, List<String> ruleStrings, StagingDbSnapshotInfo stagingDbSnapshotInfo)
          throws IOException {
    testToSucceed(ruleStrings, dsUri, null, null, stagingDbSnapshotInfo, true, null);
  }

  static void testHiveToHive(TableInfo tableInfo, List<String> ruleStrings, StagingDbSnapshotInfo stagingDbSnapshotInfo)
          throws IOException {
    testToSucceed(ruleStrings, null, null, tableInfo, stagingDbSnapshotInfo, true, null);
  }
}

