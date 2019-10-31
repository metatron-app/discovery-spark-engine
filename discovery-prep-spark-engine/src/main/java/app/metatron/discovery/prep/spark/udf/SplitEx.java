package app.metatron.discovery.prep.spark.udf;

import org.apache.spark.sql.api.java.UDF4;

public class SplitEx implements UDF4<String, String, Integer, Integer, String> {

  @Override
  public String call(String coldata, String on, Integer nth, Integer limit) throws Exception {
    int begin = 0;

    // When on is " ":
    // "Phoenix" -> "Phoenix", NULL
    // "San Francisco" -> "San", "Francisco"
    // "New York City" -> "New", "York City"

    // Skip as n
    for (int i = 0; i < nth; i++) {
      int idx = coldata.indexOf(on, begin);
      if (idx == -1) {
        return null;
      }
      begin = idx + on.length();
    }

    if (nth == limit) {
      return coldata.substring(begin);
    }

    // Now, it's the new column's value.
    int end = coldata.indexOf(on, begin);
    if (end == -1) {
      return coldata.substring(begin);
    }

    return coldata.substring(begin, end);
  }
}
