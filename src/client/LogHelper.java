package client;

import file.Index;
import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;

import java.io.*;

public class LogHelper {
    private Path logPath = new Path("/opt/localdisk/log.out");
    private FileSystem fileSystem;
    private SequenceFile.Writer writer;
    private SequenceFile.Reader reader;

    public LogHelper(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public void writeLog(Key key, Value value) throws IOException {
        ensureWriterOpen();
        writer.append(key, value);
    }

    public void readLogs(BufferedWriter bufferedWriter) throws IOException {
        ensureReaderOpen();

        if (reader == null) {
            //no logs
            return;
        }
        boolean more = false;
        do {
            Key bufKey = new Key();
            Value bufValue = new Value();
            more = reader.next(bufKey, bufValue);
            bufferedWriter.restore(bufKey, bufValue);
        } while (more);
    }

    private void ensureWriterOpen() throws IOException {
        if (writer == null) {
            writer = SequenceFile.createWriter(fileSystem, new Configuration(fileSystem.getConf()), logPath, Key.class, Value.class);
        }
    }

    private void ensureReaderOpen() throws IOException {
        if (reader == null) {
            if (fileSystem.exists(logPath)) {
                reader = new SequenceFile.Reader(fileSystem, logPath, new Configuration(fileSystem.getConf()));
            }
        }
    }

    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void refresh() throws IOException {
        writer.close();
        writer = null;
        fileSystem.delete(logPath, true);
    }
}
