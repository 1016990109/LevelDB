package cn.edu.nju.client;

import cn.edu.nju.LevelDB;
import cn.edu.nju.file.Index;
import cn.edu.nju.file.Key;
import cn.edu.nju.file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.UUID;

public class FileWriter extends Writer {

    private static final int MAX_DELETE_RETRIES = 10;

    private final FileSystem fileSystem;
    private final Path writerPath;
    private Index.Writer writer;
    private Key firstKey = new Key();
    private Key prevKey = new Key();
    private Path currentOutputStreamPath;
    private int blockSize = 128 * 1024;//default 128M
    private Metadata metaData = new Metadata();
    private Progressable progress = new Progressable() {
        @Override
        public void progress() {

        }
    };

    public FileWriter(FileSystem fileSystem, Path writerPath)
            throws IOException {
        //writerPath="/tmp",dataPath="/data"
        this.fileSystem = fileSystem;
        this.writerPath = writerPath;
        fileSystem.mkdirs(writerPath);
    }

    public void write(Key key, Value value) throws IOException {
        ensureOpen(key);
        writer.write(key, value);
        prevKey.set(key);
    }

    public void rollback() throws IOException {
        closeOutputStream();
        fileSystem.delete(new Path("/tmp"), true);
        fileSystem.delete(new Path("/generation"), true);
    }

    private void ensureOpen(Key key) throws IOException {
        if (writer == null) {
            open(key);
        } else if (!isInSortOrder(key)) {
            open(key);
        }
    }

    private boolean isInSortOrder(Key key) {
        if (key.compareTo(prevKey) < 0) {
            return false;
        } else {
            return true;
        }
    }

    private void open(Key key) throws IOException {
        closeOutputStream();
        firstKey.set(key);
        //写真实数据的路径，往/tmp/b603152b-475a-4937-be6a-bfef1d7783d4/data中写数据
        currentOutputStreamPath = new Path(writerPath, UUID.randomUUID().toString());
        writer = new Index.Writer(fileSystem, new Configuration(fileSystem.getConf()), new Path(currentOutputStreamPath,
                LevelDB.DATA), progress, metaData, blockSize);
    }

    public void closeOutputStream() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    private boolean delete(Path p) throws IOException {
        int deleteTries = 0;
        boolean result = false;
        while (!(result = fileSystem.delete(p, true)) && deleteTries < MAX_DELETE_RETRIES) {
            deleteTries++;
        }
        if (!result) {
            System.out.println(("Marking [" + p + "] deleted"));
            fileSystem.createNewFile(new Path(p, LevelDB.DELETE));
        }
        return result;
    }

    public Path getCurrentWritePath() {
        return this.currentOutputStreamPath;
    }
}