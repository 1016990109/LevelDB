import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Hello
{

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem.exists(new Path("hdfs://192.168.73.128//")));
    }
}
