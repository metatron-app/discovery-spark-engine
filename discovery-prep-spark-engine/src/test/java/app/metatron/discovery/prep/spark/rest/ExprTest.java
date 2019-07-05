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

    String dsPath = "/tmp/dataprep/uploads/sales_named.csv";
    String ssPath = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDuplicate() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("derive value: `sale_price` as: `duplicated_sale_price`");

    String dsPath = "/tmp/dataprep/uploads/sales_named.csv";
    String ssPath = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }

  @Test
  public void testDerive() {
    List<String> ruleStrings = new ArrayList();

    ruleStrings.add("header rownum: 1");
    ruleStrings.add("settype col: `sale_price` type: Double");
    ruleStrings.add("derive value: `sale_price` * 100 as: after_inflation");

    String dsPath = "/tmp/dataprep/uploads/sales_named.csv";
    String ssPath = "/tmp/dataprep/snapshots/sales.snapshot.csv";

    TestUtil.testCsvToCsv(dsPath, ruleStrings, ssPath);
  }
}
