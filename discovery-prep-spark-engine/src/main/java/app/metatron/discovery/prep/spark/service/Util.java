package app.metatron.discovery.prep.spark.service;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Util {
  private static boolean existsLocal(String path) {
    File f = new File(path);
    return f.exists();
  }

  public static Configuration getHadoopConf(String hadoopConfDir) throws IOException {
    if (hadoopConfDir == null) {
      return null;
    }

    String coreSite = hadoopConfDir + File.separator + "core-site.xml";

    if (!existsLocal(coreSite)) {
      throw new IOException("File not found: " + coreSite);
    }

    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource(new Path(coreSite));

    return hadoopConf;
  }
}
