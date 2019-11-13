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

package app.metatron.discovery.prep.spark.service;

import static app.metatron.discovery.prep.spark.service.PropertyConstant.ETL_MAX_FETCH_SIZE;
import static app.metatron.discovery.prep.spark.service.PropertyConstant.ETL_SPARK_APP_NAME;
import static app.metatron.discovery.prep.spark.service.PropertyConstant.ETL_SPARK_MASTER;
import static app.metatron.discovery.prep.spark.service.PropertyConstant.ETL_SPARK_WAREHOUSE_DIR;
import static app.metatron.discovery.prep.spark.service.PropertyConstant.ETL_TIMEOUT;
import static app.metatron.discovery.prep.spark.service.PropertyConstant.STORAGE_STAGEDB_METASTORE_URI;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Header;
import app.metatron.discovery.prep.parser.preparation.rule.Join;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.parser.preparation.rule.Union;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Constant;
import app.metatron.discovery.prep.parser.preparation.rule.expr.Expression;
import app.metatron.discovery.prep.spark.PrepTransformer;
import app.metatron.discovery.prep.spark.util.Callback;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DiscoveryPrepSparkEngineService {

  private static Logger LOGGER = LoggerFactory.getLogger(DiscoveryPrepSparkEngineService.class);

  @Autowired
  FileService fileService;

  @Autowired
  StagingDbService stagingDbService;

  private Integer timeout;
  private Integer maxFetchSize;
  private Callback callback;

  private Map<String, Dataset<Row>> cache = new HashMap();

  private void setPrepPropertiesInfo(Map<String, Object> prepPropertiesInfo) {
    timeout = (Integer) prepPropertiesInfo.get(ETL_TIMEOUT);
    maxFetchSize = (Integer) prepPropertiesInfo.get(ETL_MAX_FETCH_SIZE);

    SparkUtil.setAppName((String) prepPropertiesInfo.get(ETL_SPARK_APP_NAME));
    SparkUtil.setMasterUri((String) prepPropertiesInfo.get(ETL_SPARK_MASTER));
    SparkUtil.setMetastoreUris((String) prepPropertiesInfo.get(STORAGE_STAGEDB_METASTORE_URI));
    if (SparkUtil.getMetastoreUris() != null) {
      SparkUtil.setWarehouseDir((String) prepPropertiesInfo.get(ETL_SPARK_WAREHOUSE_DIR));
      assert SparkUtil.getMetastoreUris() != null;
    }

  }

  private void putStackTraceIntoCustomField(String ssId, Exception e) {
    StringBuffer sb = new StringBuffer();

    for (StackTraceElement ste : e.getStackTrace()) {
      sb.append("\n");
      sb.append(ste.toString());
    }
    callback.updateSnapshot(ssId, "custom", "{'fail_msg':'" + sb.toString() + "'}");
  }

  public void run(Map<String, Object> args)
          throws AnalysisException, IOException, URISyntaxException, InterruptedException {
    LOGGER.info("run(): started");

    // 1. Prepare the arguments and settings
    Map<String, Object> prepPropertiesInfo = (Map<String, Object>) args.get("prepProperties");
    Map<String, Object> dsInfo = (Map<String, Object>) args.get("datasetInfo");
    Map<String, Object> snapshotInfo = (Map<String, Object>) args.get("snapshotInfo");
    Map<String, Object> callbackInfo = (Map<String, Object>) args.get("callbackInfo");

    String dsId = (String) dsInfo.get("origTeddyDsId");

    String ssId = (String) snapshotInfo.get("ssId");
    String ssName = (String) snapshotInfo.get("ssName");
    String ssType = (String) snapshotInfo.get("ssType");
    String launchTime = (String) snapshotInfo.get("launchTime");

    LOGGER.info("run(): dsId={} ssName={} launchTime={} ssType={}", dsId, ssName, launchTime, ssType);

    setPrepPropertiesInfo(prepPropertiesInfo);

    fileService.setPrepPropertiesInfo(prepPropertiesInfo);
    stagingDbService.setPrepPropertiesInfo(prepPropertiesInfo);

    callback = new Callback(callbackInfo, ssId);
    callback.updateSnapshot(ssId, "ruleCntTotal", String.valueOf(countAllRules(dsInfo)));
    callback.updateAsRunning(ssId);

    if (ssType == null) {
      callback.updateAsFailed(ssId);
      throw new IllegalArgumentException("The request does not contain snapshot type");
    }

    // 2. Transform the DataFrame with rule strings
    transformDf(ssId, dsInfo);

    // 3. Write the transformed DataFrame.
    Dataset<Row> df = cache.get(dsId);
    callback.updateAsWriting(ssId);

    switch (ssType) {
      case "URI":
        long totalLines = fileService.createSnapshot(df, snapshotInfo);
        callback.updateSnapshot(ssId, "totalLines", String.valueOf(totalLines));
        break;
      case "STAGING_DB":
        callback.updateAsTableCreating(ssId);
        totalLines = stagingDbService.createSnapshot(df, snapshotInfo);
        callback.updateSnapshot(ssId, "totalLines", String.valueOf(totalLines));
        break;
      default:
        assert false : ssType;
    }

    // Why should we write colDescs at the end of snapshot generation?
    // It could be done by the start when the snapshot informations are filled, like ssName, ssType, etc.
    callback.updateSnapshot(ssId, "custom", "Not implemented in spark engine");   // colDescs

    String finishTime = DateTime.now(DateTimeZone.UTC).toString();
    callback.updateSnapshot(ssId, "finishTime", finishTime);

    LOGGER.info("run(): result={} finishTime={}", "SUCCEEDED", finishTime);
    callback.updateAsSucceeded(ssId);
  }

  private void transformDf(String ssId, Map<String, Object> dsInfo)
          throws IOException, AnalysisException, URISyntaxException, InterruptedException {
    try {
      transformRecursive(ssId, dsInfo);
    } catch (CancellationException e) {
      handleException(ssId, "CANCELED", e);
      throw e;
    } catch (URISyntaxException e) {
      handleException(ssId, "FAILED", e);
      throw e;
    }

    LOGGER.info("transformDf(): done: ssId={}", ssId);
  }

  private void transformRecursive(String ssId, Map<String, Object> dsInfo)
          throws IOException, URISyntaxException, AnalysisException {
    String dsId = (String) dsInfo.get("origTeddyDsId");

    // We don't need to convert dataset into a full dataset. (cf. Embedded engine)
    List<String> ruleStrings = (List<String>) dsInfo.get("ruleStrings");
    boolean header = removeUnusedRules(ruleStrings);
    Dataset<Row> df = createStage0(dsInfo, header);

    // Transform upstreams first.
    List<Map<String, Object>> upstreamDatasetInfos = (List<Map<String, Object>>) dsInfo.get("upstreamDatasetInfos");
    if (upstreamDatasetInfos != null) {
      for (Map<String, Object> upstreamDatasetInfo : upstreamDatasetInfos) {
        transformRecursive(ssId, upstreamDatasetInfo);
      }
    }

    // Transform
    PrepTransformer transformer = new PrepTransformer();
    for (String ruleString : ruleStrings) {
      df = transformer.applyRule(df, ruleString, getSlaveDfs(ruleString));
      callback.incrRuleCntDone(ssId);
    }
    cache.put(dsId, df);
  }

  private long countAllRules(Map<String, Object> dsInfo) {
    long ruleCntTotal = 0L;

    List<Map<String, Object>> upstreamDatasetInfos = (List<Map<String, Object>>) dsInfo.get("upstreamDatasetInfos");
    if (upstreamDatasetInfos != null) {
      for (Map<String, Object> upstreamDatasetInfo : upstreamDatasetInfos) {
        ruleCntTotal += countAllRules(upstreamDatasetInfo);
      }
    }

    return ruleCntTotal + ((List<String>) dsInfo.get("ruleStrings")).size();
  }

  private Dataset<Row> createStage0(Map<String, Object> dsInfo, boolean header) throws IOException, URISyntaxException {
    Dataset<Row> df;
    String dsId = ((String) dsInfo.get("origTeddyDsId"));
    LOGGER.debug("createStage0(): dsId={}", dsId);

    String importType = (String) dsInfo.get("importType");
    switch (importType) {
      case "UPLOAD":
      case "URI":
        df = fileService.createStage0(dsInfo, header);
        break;

      case "STAGING_DB":
        df = stagingDbService.createStage0(dsInfo);
        break;

      case "DATABASE":
      default:
        throw new IllegalArgumentException("createStage0(): not supported importType: " + importType);
    }

    LOGGER.debug("createStage0(): end");
    return df;
  }

  static public List<String> getSlaveDsIds(String ruleString) {
    Rule rule = new RuleVisitorParser().parse(ruleString);

    switch (rule.getName()) {
      case "join":
        return getLiteralList(((Join) rule).getDataset2());
      case "union":
        return getLiteralList(((Union) rule).getDataset2());
      default:
        return null;
    }
  }

  static private List<String> getLiteralList(Expression expr) {
    List<String> literals = null;
    if (expr instanceof Constant.StringExpr) {
      literals = new ArrayList<>();
      literals.add(((Constant.StringExpr) expr).getEscapedValue());
    } else if (expr instanceof Constant.ArrayExpr) {
      literals = ((Constant.ArrayExpr) expr).getValue();
      for (int i = 0; i < literals.size(); i++) {
        literals.set(i, literals.get(i).replaceAll("'", ""));
      }
    } else {
      assert false : expr;
    }
    return literals;
  }

  private List<Dataset<Row>> getSlaveDfs(String ruleString) {
    List<Dataset<Row>> slaveDfs = new ArrayList();

    List<String> slaveDsIds = getSlaveDsIds(ruleString);
    if (slaveDsIds != null) {
      for (String slaveDsId : slaveDsIds) {
        slaveDfs.add(cache.get(slaveDsId));
      }
    }

    return slaveDfs;
  }

  private boolean removeUnusedRules(List<String> ruleStrings) {
    if (ruleStrings.size() > 0 && ruleStrings.get(0).startsWith("create")) {
      ruleStrings.remove(0);
    }

    if (ruleStrings.size() > 0) {
      String ruleString = ruleStrings.get(0);
      Rule rule = (new RuleVisitorParser()).parse(ruleString);

      if (rule instanceof Header) {
        Header header = (Header) rule;
        if (header.getRownum() == null || header.getRownum().longValue() == 1) {
          ruleStrings.remove(0);
          return true;
        }
      }
    }
    return false;
  }

  private void handleException(String ssId, String status, Exception e) {
    LOGGER.info("transformDf(): stopped: ssid={} status={}", ssId, status);
    putStackTraceIntoCustomField(ssId, e);
    callback.updateStatus(ssId, status);
  }
}
