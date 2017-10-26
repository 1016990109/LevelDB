package cn.edu.nju.client;

import cn.edu.nju.MyProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

import java.io.*;
import java.util.Map;

public class LogHelper {
    private String logPath = "/opt/localdisk/log.out";
    private MyProcessor processor;
    private ObjectOutputStream writer;
    private ObjectInputStream reader;

    public LogHelper(MyProcessor processor) {
        this.processor = processor;
    }

    public void writeLog(String key, Map<String, String> value) throws IOException {
        ensureWriterOpen();
        writer.writeObject(key);
        writer.writeObject(value);
        writer.flush();
    }

    public void readLogs() throws IOException, ClassNotFoundException {
        ensureReaderOpen();

        if (reader == null) {
            //no logs
            return;
        }

        try {
            while (true) {
                String key = (String) reader.readObject();
                Map<String, String> value = (Map<String, String>) reader.readObject();

                processor.put(key, value, false);
            }
        } catch (EOFException e) {

        } finally {
            reader.close();
        }

    }

    private void ensureWriterOpen() throws IOException {
        if (writer == null) {
            File file = new File(logPath);
            FileOutputStream fos = new FileOutputStream(file, true);
//            if (file.length() > 0) {
//                writer = new NoHeaderObjectOutputStream(fos);
//            } else {
                writer = new ObjectOutputStream(fos);
//            }
        }
    }

    private void ensureReaderOpen() throws IOException {
        if (reader == null) {
            File file = new File(logPath);

            if (file.exists()) {
                reader = new ObjectInputStream(new FileInputStream(file));
            }
        }
    }

    private static CompressionCodec getCodec(Configuration conf) {
        if (SnappyCodec.isNativeCodeLoaded()) {
            return new SnappyCodec();
        }
        return new DeflateCodec();
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
        File file = new File(logPath);

        if (file.exists() && file.isFile()) {
            file.delete();
        }
    }
}
