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

package app.metatron.discovery.prep.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

  public static StructType getSchemaFromJson(String storedUri)
          throws URISyntaxException, IOException {
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

  public static long writeJson(Dataset<Row> df, String strUri, Configuration conf, int limitRows)
          throws URISyntaxException, IOException {
    Writer writer;
    URI uri;
    long totalLines = 0L;

    LOGGER.debug("writeJson(): strUri={} conf={}", strUri, conf);

    try {
      uri = new URI(strUri);
    } catch (URISyntaxException e) {
      LOGGER.error("writeJson(): URISyntaxException: strUri={}", strUri);
      throw e;
    }

    switch (uri.getScheme()) {
      case "hdfs":
        if (conf == null) {
          LOGGER.error(
                  "writeJson(): Required property missing: check polaris.dataprep.hadoopConfDir: strUri={}",
                  strUri);
        }
        Path path = new Path(uri);

        FileSystem hdfsFs;

        try {
          hdfsFs = FileSystem.get(conf);
        } catch (IOException e) {
          LOGGER.error(
                  "writeJson(): Cannot get file system: check polaris.dataprep.hadoopConfDir: strUri={}",
                  strUri);
          throw e;
        }

        FSDataOutputStream hos;
        try {
          hos = hdfsFs.create(path);
        } catch (IOException e) {
          LOGGER.error(
                  "writeJson(): Cannot create a file: polaris.dataprep.hadoopConfDir: strUri={}",
                  strUri);
          throw e;
        }

        writer = new BufferedWriter(CsvUtil.getWriter(hos));
        break;

      case "file":
        File file = new File(uri);
        File dirParent = file.getParentFile();
        assert dirParent != null : uri;

        if (!dirParent.exists()) {
          if (!dirParent.mkdirs()) {
            String errmsg = "writeJson(): Cannot create a directory: " + strUri;
            LOGGER.error(errmsg);
            throw new IOException(errmsg);
          }
        }

        FileOutputStream fos;
        try {
          fos = new FileOutputStream(file);
        } catch (FileNotFoundException e) {
          LOGGER.error(
                  "writeJson(): FileNotFoundException: Check the permission of snapshot directory: strUri={}",
                  strUri);
          throw e;
        }

        writer = new BufferedWriter(CsvUtil.getWriter(fos));
        break;

      default:
        String errmsg = "writeJson(): Unsupported URI scheme: " + strUri;
        LOGGER.error(errmsg);
        throw new IOException(errmsg);
    }

    String[] colNames = df.columns();
    StringBuilder sb = new StringBuilder();
    ObjectMapper mapper = new ObjectMapper();

    Iterator iter = df.toLocalIterator();
    while (iter.hasNext()) {
      Row row = (Row) iter.next();
      sb.append("{");
      for (int i = 0; i < row.size(); i++) {
        if (row.get(i) == null) {
          continue;
        }
        sb.append("\"");
        sb.append(colNames[i]);
        sb.append("\"");

        sb.append(":");
        sb.append(mapper.writeValueAsString(row.get(i)));
        sb.append(",");
      }

      sb.setLength(sb.length() - 1);  // At least one column should not be null
      sb.append("}\n");

      writer.write(sb.toString());
      if (++totalLines >= limitRows) {
        break;
      }

      sb.setLength(0);
    }

    writer.close();
    return totalLines;
  }
}
