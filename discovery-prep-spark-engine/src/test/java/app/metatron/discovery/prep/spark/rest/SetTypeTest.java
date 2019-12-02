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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class SetTypeTest {

  @Test
  public void testSetTypeLong() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("rename col: `Population_`, `Murder_`, `Forcible_Rape_`, `Robbery_`, `Aggravated_Assault_`, "
            + "`Burglary_`, `Larceny_Theft_`, `Vehicle_Theft_` "
            + "to: 'Population', 'Murder', 'Forcible_Rape', 'Robbery', 'Aggravated_Assault', "
            + "'Burglary', 'Larceny_Theft', 'Vehicle_Theft'");
    ruleStrings.add("settype col: `Population` type: Long");

    String dsUri = TestUtil.getResourcePath("csv/crime.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testSetTypeDouble() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/crime.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testSetTypeDate() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `contract_date` type: Timestamp format: 'yyyy-MM-dd\'T\'HH:mm:ssz'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testSetTypeTimestamp() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings
            .add("settype col: `contract_date` type: Timestamp format: 'yyyy-MM-dd\'T\'HH:mm:ssz'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testSetTypeTimestampFormat() throws IOException {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings
            .add("settype col: `contract_date` type: Timestamp format: 'yyyy-MM-dd\'T\'HH:mm:ssz'");
    ruleStrings.add("settype col: `contract_date` type: String format: 'yyyy/MM/dd'");

    String dsUri = TestUtil.getResourcePath("csv/sales_named.csv");
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToFile(dsUri, ruleStrings, ssUri);
  }
}
