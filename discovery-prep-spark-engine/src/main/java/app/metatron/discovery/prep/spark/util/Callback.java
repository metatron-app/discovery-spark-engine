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

package app.metatron.discovery.prep.spark.util;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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

    updateSnapshot(ssId, "ruleCntDone", String.valueOf(cnt));
  }

  public void updateSnapshot(String ssId, String attr, String value) {
    LOGGER.info("updateSnapshot(): ssId={}: update {} as {}", ssId, attr, value);

    if (port == 0) {
      // When run test cases, port is set to 0.
      return;
    }

    URI snapshot_uri = UriComponentsBuilder.newInstance()
            .scheme("http")
            .host("localhost")
            .port(port)
            .path("/api/preparationsnapshots/")
            .path(ssId)
            .build().encode().toUri();

    LOGGER.info("updateSnapshot(): REST URI=" + snapshot_uri);
    LOGGER.info("attr={} value={}", attr, value);

    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.add("Accept", "application/json, text/plain, */*");
    headers.add("Authorization", oauth_token);

    HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
    RestTemplate restTemplate = new RestTemplate(requestFactory);

    Map<String, String> patchItems = new HashMap<>();
    patchItems.put(attr, value);

    HttpEntity<Map<String, String>> entity2 = new HttpEntity<>(patchItems, headers);
    ResponseEntity<String> responseEntity;
    responseEntity = restTemplate.exchange(snapshot_uri, HttpMethod.PATCH, entity2, String.class);

    LOGGER.info("updateSnapshot(): done with statusCode " + responseEntity.getStatusCode());
  }

  public void updateAsRunning(String ssId) {
    cancelCheck(ssId);
    updateSnapshot(ssId, "status", "RUNNING");
  }

  public void updateAsWriting(String ssId) {
    cancelCheck(ssId);
    updateSnapshot(ssId, "status", "WRITING");
  }

  public void updateAsTableCreating(String ssId) {
    cancelCheck(ssId);
    updateSnapshot(ssId, "status", "TABLE_CREATING");
  }

  public void updateAsSucceeded(String ssId) {
    updateSnapshot(ssId, "status", "SUCCEEDED");
    snapshotRuleDoneCnt.remove(ssId);
  }

  public void updateAsFailed(String ssId) {
    updateSnapshot(ssId, "status", "FAILED");
    snapshotRuleDoneCnt.remove(ssId);
  }

  public void updateAsCanceled(String ssId) {
    updateSnapshot(ssId, "status", "CANCELED");
    snapshotRuleDoneCnt.remove(ssId);
  }

  // synchronize with what?
  synchronized public void cancelCheck(String ssId) throws CancellationException {
    // Not implemented
  }

  public void updateStatus(String ssId, String status) {
    updateSnapshot(ssId, "status", status);

    if (status.equals("SUCCEEDED") || status.equals("FAILED") || status.equals("CANCELED")) {
      updateSnapshot(ssId, "finishTime", DateTime.now(DateTimeZone.UTC).toString());
    }
  }
}
