package app.metatron.discovery.prep.spark.util;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

public class Callback {

  private static Logger LOGGER = LoggerFactory.getLogger(Callback.class);

  int port;
  String oauth_token;

  private Map<String, Integer> snapshotRuleDoneCnt;

  public Callback() {
  }

  public Callback(Map<String, Object> callbackInfo, String ssId) {
    port = Integer.valueOf((String) callbackInfo.get("port"));
    oauth_token = (String) callbackInfo.get("oauth_token");

    snapshotRuleDoneCnt = new HashMap();
    snapshotRuleDoneCnt.put(ssId, 0);
  }

  public void incrRuleCntDone(String ssId) {
    int cnt = snapshotRuleDoneCnt.get(ssId);
    snapshotRuleDoneCnt.put(ssId, ++cnt);

    updateSnapshot("ruleCntDone", String.valueOf(cnt), ssId);
  }

  public void updateSnapshot(String colname, String value, String ssId) {
    LOGGER.debug("updateSnapshot(): ssId={}: update {} as {}", ssId, colname, value);

    if (port == 0) {
      return;
    }

    URI snapshot_uri = UriComponentsBuilder.newInstance()
        .scheme("http")
        .host("localhost")
        .port(port)
        .path("/api/preparationsnapshots/")
        .path(ssId)
        .build().encode().toUri();

    LOGGER.debug("updateSnapshot(): REST URI=" + snapshot_uri);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.add("Accept", "application/json, text/plain, */*");
    headers.add("Authorization", oauth_token);

    HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
    RestTemplate restTemplate = new RestTemplate(requestFactory);

    Map<String, String> patchItems = new HashMap<>();
    patchItems.put(colname, value);

    HttpEntity<Map<String, String>> entity2 = new HttpEntity<>(patchItems, headers);
    ResponseEntity<String> responseEntity;
    responseEntity = restTemplate.exchange(snapshot_uri, HttpMethod.PATCH, entity2, String.class);

    LOGGER.debug("updateSnapshot(): done with statusCode " + responseEntity.getStatusCode());
  }

  public void updateAsRunning(String ssId) {
    cancelCheck(ssId);
    updateSnapshot("status", "RUNNING", ssId);
  }

  public void updateAsWriting(String ssId) {
    cancelCheck(ssId);
    updateSnapshot("status", "WRITING", ssId);
  }

  public void updateAsTableCreating(String ssId) {
    cancelCheck(ssId);
    updateSnapshot("status", "TABLE_CREATING", ssId);
  }

  public void updateAsSucceeded(String ssId) {
    updateSnapshot("status", "SUCCEEDED", ssId);
    snapshotRuleDoneCnt.remove(ssId);
  }

  public void updateAsFailed(String ssId) {
    updateSnapshot("status", "FAILED", ssId);
    snapshotRuleDoneCnt.remove(ssId);
  }

  public void updateAsCanceled(String ssId) {
    updateSnapshot("status", "CANCELED", ssId);
    snapshotRuleDoneCnt.remove(ssId);
  }

  // synchronize with what?
  synchronized public void cancelCheck(String ssId) throws CancellationException {
    // Not implemented
  }
}
