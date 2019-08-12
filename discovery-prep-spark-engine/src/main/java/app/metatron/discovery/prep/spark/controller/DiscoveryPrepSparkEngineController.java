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

package app.metatron.discovery.prep.spark.controller;

import static app.metatron.discovery.prep.spark.util.SparkUtil.stopSession;

import app.metatron.discovery.prep.spark.service.DiscoveryPrepSparkEngineService;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DiscoveryPrepSparkEngineController {

  private static Logger LOGGER = LoggerFactory.getLogger(DiscoveryPrepSparkEngineController.class);

  @Autowired
  DiscoveryPrepSparkEngineService service;

  Map<String, Object> buildSucceededResponse() {
    Map<String, Object> response = new HashMap();
    response.put("result", "SUCCEEDED");
    return response;
  }

  Map<String, Object> buildFailedResponse(Throwable e) {
    LOGGER.error("run(): failed with exception:", e);

    Map<String, Object> response = new HashMap();
    response.put("result", "FAILED");
    response.put("exception", e.getClass().getName());
    response.put("message", e.getMessage());
    response.put("cause", e.getCause());

    StringWriter sw = new StringWriter();
    e.printStackTrace(new PrintWriter(sw));
    response.put("trace", sw.toString());

    return response;
  }

  @RequestMapping(method = RequestMethod.POST, path = "/run", consumes = "application/JSON", produces = "application/JSON")
  @ResponseBody
  public Map<String, Object> run(@RequestBody Map<String, Object> request) {
    Map<String, Object> response;

    try {
      service.run(request);
      response = buildSucceededResponse();
    } catch (Throwable e) {
      response = buildFailedResponse(e);
    }

    stopSession();
    return response;
  }

  @RequestMapping(method = RequestMethod.POST, path = "/ping", consumes = "application/JSON", produces = "application/JSON")
  @ResponseBody
  public Map<String, Object> ping() {
    return buildSucceededResponse();
  }
}
