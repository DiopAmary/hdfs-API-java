import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.IOException;
import java.net.URL;

public class Main {
    public static void main(String[] args) throws Exception {
        URL url = new URL("https://www.ncei.noaa.gov/data/global-hourly/archive/csv/1930.tar.gz");
        Path path = new Path("/user/dioppp/homework1/1930.tar.gz");
        Configuration config = new Configuration();
        config.set("dfs.client.use.datanode.hostname","true");
        config.set("fs.defaultFS", "hdfs://hadoop-master:9000/");
        FileSystem fs = FileSystem.get(config);
        download(fs, url, path);
    }
    static void download(FileSystem fs, URL url, Path filePath) {
        try {
            File fSrc=new File("/root/weather/1930.tar.gz");
            FileUtils.copyURLToFile(
                    url,
                    fSrc,100000,100000);
            fs.copyFromLocalFile(new Path(fSrc.getAbsolutePath()), filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
