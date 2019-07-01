package app.metatron.discovery.prep.spark.rest;

import static com.jayway.restassured.RestAssured.given;
import static org.junit.Assert.assertEquals;

import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.response.Response;
import java.util.Map;
import org.apache.commons.httpclient.HttpStatus;
import org.junit.Test;
import org.testng.collections.Maps;


public class RestTest {

  public static String BASE_URL = "http://localhost:8080";

  @Test
  public void parseRule() {
    Map<String, Object> map = Maps.newHashMap();
    map.put("ruleString", "drop col: `column`");

    Response response = given()
        .contentType(ContentType.JSON)
        .accept(ContentType.JSON)
        .when()
        .content(map)
        .post(BASE_URL + "/parse")
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .extract()
        .response();

    assertEquals(response.path("result"), "Drop{col=column}");
  }
}

