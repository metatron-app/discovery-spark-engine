package app.metatron.discovery.prep.spark.controller;

import app.metatron.discovery.prep.spark.service.DiscoveryPrepSparkEngineService;
import java.io.IOException;
import java.net.URISyntaxException;
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

  @RequestMapping(method = RequestMethod.POST, path = "/parse", consumes = "application/JSON", produces = "application/JSON")
  public
  @ResponseBody
  Map<String, Object> parseRule(@RequestBody Map<String, Object> request) {
    return service.parseRule((String) request.get("ruleString"));
  }

  @RequestMapping(method = RequestMethod.POST, path = "/run", consumes = "application/JSON", produces = "application/JSON")
  public
  @ResponseBody
  Map<String, Object> run(@RequestBody Map<String, Object> request) {
    Map<String, Object> response = new HashMap();

    try {
      service.run(request);
      response.put("result", "OK");
    } catch (URISyntaxException e) {
      LOGGER.error("run(): URISyntaxException:", e);
      response.put("result", "FAILED");
      response.put("cause", "URISyntaxException");
    } catch (IOException e) {
      LOGGER.error("run(): IOException:", e);
      response.put("result", "FAILED");
      response.put("cause", "IOException");
    }

    return response;
  }
}
