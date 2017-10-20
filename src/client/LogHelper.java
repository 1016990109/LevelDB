package client;

import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;

import java.io.*;

public class LogHelper {
    private FileSystem fileSystem;
    private SequenceFile.Writer writer;
    private SequenceFile.Reader reader;
    private SequenceFile.Metadata metaData = new SequenceFile.Metadata();
    private Progressable progress = new Progressable() {
        @Override
        public void progress() {

        }
    };

    public LogHelper(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void writeLog(Key key, Value value) throws IOException {
        ensureWriterOpen();
        writer.append(key, value);
    }

    public void readLogs(BufferedWriter bufferedWriter) throws IOException {
        boolean more = false;
        Key bufKey = new Key();
        Value bufValue = new Value();
        do {
            more = reader.next(bufKey, bufValue);
            bufferedWriter.restore(bufKey, bufValue);
        } while (more);
    }

    private void ensureWriterOpen() throws IOException {
        if (writer == null) {
            writer = SequenceFile.createWriter(fileSystem, new Configuration(fileSystem.getConf()), new Path("/log.out"), Key.class, LongWritable.class);
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
