package app.metatron.discovery.prep.spark.rest;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;


public class ExprTest {

  @Test
  public void testSet() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("set col: `sale_price` value: sale_price * 100");

    String dsUri = "/tmp/dataprep/uploads/sales_named.csv";
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDuplicate() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("derive value: `sale_price` as: `duplicated_sale_price`");

    String dsUri = "/tmp/dataprep/uploads/sales_named.csv";
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testDerive() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("derive value: `sale_price` * 100 as: after_inflation");

    String dsUri = "/tmp/dataprep/uploads/sales_named.csv";
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceLiteral() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `business`, `base` on: 'Office' with: 'Desk'");

    String dsUri = "/tmp/dataprep/uploads/sales_named.csv";
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExp() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `due` on: /\\\\d/ with: 'x'");

    String dsUri = "/tmp/dataprep/uploads/sales_named.csv";
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);
  }

  @Test
  public void testReplaceRegExpGroup() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("replace col: `due` on: /(\\\\d+).*(\\\\d+).*(\\\\d+).*/ with: '$1-$2-$3'");

    String dsUri = "/tmp/dataprep/uploads/sales_named.csv";
    String ssUri = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testFileToCsv(dsUri, ruleStrings, ssUri);
  }
}
