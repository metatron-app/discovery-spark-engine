package app.metatron.discovery.prep.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtil {

  private static Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);

  private static String readLineFromJson(String storedUri) throws URISyntaxException, IOException {
    URI uri = new URI(storedUri);
    if (uri.getScheme() == null) {
      storedUri = "file://" + storedUri;
      uri = new URI(storedUri);
    }

    BufferedReader reader;
    switch (uri.getScheme()) {
      case "file":
        File file = new File(uri);
        try {
          reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
          LOGGER.error("readLineFromJson(): FileNotFoundException: strUri={}", storedUri);
          throw e;
        }
        break;

      case "hdfs":
        Configuration conf = new Configuration();
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/core-site.xml"));

        Path path = new Path(uri);
        FileSystem fs = path.getFileSystem(conf);

        FSDataInputStream in = fs.open(path);
        reader = new BufferedReader(new InputStreamReader(in));
        break;

      default:
        throw new IOException("Wrong uri scheme: " + uri);
    }

    String line;
    try {
      line = reader.readLine();
    } catch (IOException e) {
      LOGGER.error("readLineFromJson(): IOException: strUri={}", storedUri);
      throw e;
    }

    return line;
  }

  private static class JsonColumn implements Comparable<JsonColumn> {

    private String colName;
    private int pos;

    public JsonColumn(String colName, int pos) {
      this.colName = colName;
      this.pos = pos;
    }

    @Override
    public int compareTo(JsonColumn jsonColumn) {
      return this.pos - jsonColumn.pos;
    }
  }

  public static StructType getSchemaFromJson(String storedUri) throws URISyntaxException, IOException {
    String line = readLineFromJson(storedUri);
    ObjectMapper mapper = new ObjectMapper();

    Map<String, String> map;
    try {
      map = mapper.readValue(line, Map.class);
    } catch (IOException e) {
      LOGGER.error("getSchemaFromJson(): IOException: strUri={}", storedUri);
      throw e;
    }

    JsonColumn[] columns = new JsonColumn[map.size()];
    int i = 0;

    for (String key : map.keySet()) {
      int pos = line.indexOf("\"" + key + "\"");
      pos = (pos >= 0) ? pos : line.indexOf("'" + key + "'");
      assert pos >= 0;

      columns[i++] = new JsonColumn(key, pos);
    }
    Arrays.sort(columns);

    StructField[] fields = new StructField[map.size()];
    for (i = 0; i < columns.length; i++) {
      fields[i] = DataTypes.createStructField(columns[i].colName, DataTypes.StringType, true);
    }

    return DataTypes.createStructType(fields);
  }
}
