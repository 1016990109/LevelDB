package test;

import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URL;
import java.util.HashMap;

public class LogTest {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        //本地localdisk写log
//        FileSystem fileSystem = FileSystem.getLocal(conf);
//        FSDataOutputStream stream = fileSystem.create(new Path("/opt/localdisk/log.out"), false);
//        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, new Configuration(fileSystem.getConf()), new Path("/opt/localdisk/log.out"),
//                Key.class, Value.class, SequenceFile.CompressionType.NONE, getCodec(conf), new Progressable() {
//                    @Override
//                    public void progress() {
//
//                    }
//                }, new SequenceFile.Metadata());
        ObjectOutputStream out=new ObjectOutputStream(new FileOutputStream("/opt/localdisk/log.out"));

        Key key = new Key();
        key.setRowId("test");
        Value value = new Value();
        value.setData("test");
        out.writeObject("test");
        out.writeObject(new HashMap<>());
        out.flush();
//        writer.append(key, value);
//        IOUtils.closeStream(writer);
//
//        writer.append(key, value);
//        out.close();
    }

    private static CompressionCodec getCodec(Configuration conf) {
        if (SnappyCodec.isNativeCodeLoaded()) {
            return new SnappyCodec();
        }
        return new DeflateCodec();
    }
}
