package cn.edu.nju.client;

import cn.edu.nju.LevelDB;
import cn.edu.nju.file.Key;
import cn.edu.nju.file.Value;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class BufferedWriter extends Writer {

    private Writer writer;
    private ConcurrentSkipListMap<Key, Value> buffer;
    private final int maxSize;
    private LevelDB client;
    private FileSystem fileSystem;
    private Path storePath;

    public BufferedWriter(FileSystem fileSystem, Path storePath, int maxSize, LevelDB client) throws IOException {
        //初始化
        this.fileSystem = fileSystem;
        this.storePath = storePath;
        this.writer = new FileWriter(fileSystem, storePath);
        this.maxSize = maxSize;
        this.client = client;
        buffer = new ConcurrentSkipListMap<>();
    }

    @Override
    public void write(Key key, Value value) throws IOException {
        buffer.put(key, value);
        flushIfNeeded();
    }

    /**
     * @throws IOException
     */
    private synchronized void flushIfNeeded() throws IOException {
        if (buffer.size() >= maxSize) {
            //clone 浅表副本，但由于Key，Value都是一次性产物，所以没毛病;初始化新的writer
            ConcurrentSkipListMap<Key, Value> copyBuffer = buffer.clone();
            FileWriter copyFileWriter = (FileWriter) writer;
            writer = new FileWriter(fileSystem, storePath);
            buffer = new ConcurrentSkipListMap<>();
            //启动新线程去写入hdfs
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        flush(copyBuffer, copyFileWriter);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    /**
     * 写入hdfs真实kv数据，多线程
     * @param flushMap
     * @param fileWriter
     * @throws IOException
     */
    private void flush(ConcurrentSkipListMap<Key, Value> flushMap, FileWriter fileWriter) throws IOException {
        if (flushMap.size() == 0) {
            return;
        }
        for (Map.Entry<Key, Value> entry : flushMap.entrySet()) {
            fileWriter.write(entry.getKey(), entry.getValue());
        }
        fileWriter.closeOutputStream();
        this.client.updateRange(flushMap.firstKey(), flushMap.lastKey(), fileWriter.getCurrentWritePath());
        this.client.refreshGeneration();
        this.client.deleteLog();
    }

    @Override
    public void rollback() throws IOException {
        buffer = new ConcurrentSkipListMap<>();
        writer.rollback();
    }

    public boolean findInCache(Key key, Value value) {
        Value tmp = buffer.get(key);
        if (tmp != null) {
            value.set(tmp);
            return true;
        } else {
            return false;
        }
    }
}
