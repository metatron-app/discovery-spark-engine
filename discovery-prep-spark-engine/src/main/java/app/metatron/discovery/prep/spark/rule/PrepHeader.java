package app.metatron.discovery.prep.spark.rule;

import app.metatron.discovery.prep.parser.preparation.rule.Header;
import app.metatron.discovery.prep.parser.preparation.rule.Rule;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PrepHeader extends PrepRule {

  public static Dataset<Row> transform(Dataset<Row> df, Rule rule) {
    Dataset<Row> newDf = df;
    String newColName;

    Header header = (Header) rule;
    Integer rownum = header.getRownum();
    if (rownum == null) {
      rownum = 1;
    }

    newDf = newDf.withColumn("rowid", org.apache.spark.sql.functions.monotonically_increasing_id());

    String[] colNames = df.columns();
    Row row = newDf.filter("rowid = " + (rownum - 1)).head();
    newDf = newDf.filter("rowid != " + (rownum - 1));

    for (int i = 0; i < colNames.length; i++) {
      if (row.get(i) != null) {
        newColName = row.get(i).toString();
      } else {
        newColName = "column" + (i + 1);
      }
      newDf = newDf.withColumnRenamed(colNames[i], newColName);
    }

    return newDf;
  }
}
