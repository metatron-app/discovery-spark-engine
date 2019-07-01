package app.metatron.discovery.prep.spark.controller;

import app.metatron.discovery.prep.spark.service.DiscoveryPrepSparkEngineService;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DiscoveryPrepSparkEngineController {

  @Autowired
  DiscoveryPrepSparkEngineService service;

  @RequestMapping(method = RequestMethod.POST, path = "/parse", consumes = "application/JSON", produces = "application/JSON")
  public @ResponseBody
  Map<String, Object> parseRule(@RequestBody Map<String, Object> request) {
    return service.parseRule((String) request.get("ruleString"));
  }
}
