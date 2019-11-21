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

package app.metatron.discovery.prep.spark;

import app.metatron.discovery.prep.parser.preparation.RuleVisitorParser;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import app.metatron.discovery.prep.spark.rule.PrepDelete;
import app.metatron.discovery.prep.spark.rule.PrepDerive;
import app.metatron.discovery.prep.spark.rule.PrepDrop;
import app.metatron.discovery.prep.spark.rule.PrepExtract;
import app.metatron.discovery.prep.spark.rule.PrepHeader;
import app.metatron.discovery.prep.spark.rule.PrepJoin;
import app.metatron.discovery.prep.spark.rule.PrepKeep;
import app.metatron.discovery.prep.spark.rule.PrepMerge;
import app.metatron.discovery.prep.spark.rule.PrepMove;
import app.metatron.discovery.prep.spark.rule.PrepRename;
import app.metatron.discovery.prep.spark.rule.PrepReplace;
import app.metatron.discovery.prep.spark.rule.PrepSet;
import app.metatron.discovery.prep.spark.rule.PrepSetType;
import app.metatron.discovery.prep.spark.rule.PrepSort;
import app.metatron.discovery.prep.spark.rule.PrepSplit;
import app.metatron.discovery.prep.spark.rule.PrepUnion;
import app.metatron.discovery.prep.spark.rule.PrepUnnest;
import app.metatron.discovery.prep.spark.util.SparkUtil;
import java.io.IOException;
import java.util.List;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrepTransformer {

  private static Logger LOGGER = LoggerFactory.getLogger(PrepTransformer.class);

  SparkSession session;
  RuleVisitorParser parser;

  public PrepTransformer() {
    session = SparkUtil.getSession();
    parser = new RuleVisitorParser();
  }

  public Dataset<Row> applyRule(Dataset<Row> df, String ruleString, List<Dataset<Row>> slaveDfs)
          throws AnalysisException, IOException {
    Rule rule = parser.parse(ruleString);

    switch (rule.getName()) {
      case "move":
        return (new PrepMove()).transform(df, rule);
      case "sort":
        return (new PrepSort()).transform(df, rule);
      case "rename":
        return (new PrepRename()).transform(df, rule);
      case "header":
        return (new PrepHeader()).transform(df, rule);
      case "drop":
        return (new PrepDrop()).transform(df, rule);
      case "keep":
        return (new PrepKeep()).transform(df, rule);
      case "delete":
        return (new PrepDelete()).transform(df, rule);
      case "settype":
        return (new PrepSetType()).transform(df, rule);
      case "set":
        // Cause SparkSQL always needs a timestamp format, we use "format" field of "set". (Embedded engine doesn't use)
        rule = parser.parse(ruleString.replace("'T'", "\\\'T\\\'"));
        return (new PrepSet()).transform(df, rule);
      case "derive":
        return (new PrepDerive()).transform(df, rule);
      case "replace":
        return (new PrepReplace()).transform(df, rule);
      case "extract":
        return (new PrepExtract()).transform(df, rule);
      case "split":
        return (new PrepSplit()).transform(df, rule);
      case "merge":
        return (new PrepMerge()).transform(df, rule);
      case "union":
        return (new PrepUnion()).transform(df, slaveDfs);
      case "join":
        return (new PrepJoin()).transform(df, rule, slaveDfs);
      case "unnest":
        return (new PrepUnnest()).transform(df, rule);
    }

    LOGGER.error("Unsupported rule: " + ruleString);
    throw new IllegalArgumentException("Unsupported rule: " + rule.getName());
  }
}
