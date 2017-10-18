package client;

import file.Index;
import file.Index.Reader;
import file.Key;
import file.Value;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class FileWriter extends Writer {

    private static final int MAX_DELETE_RETRIES = 10;

    private final static Log LOG = LogFactory.getLog(FileWriter.class);

    private final FileSystem fileSystem;
    private final Path writerPath;
    private final Path dataPath;
    private final Key readerKey = new Key();
    private final Value readerValue = new Value();
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
    private MergeReader reader;
    private boolean readerMore;
    private Collection<Reader> readers;
    private final int maxNumberOfSegmentsPerWriter;

    public FileWriter(FileSystem fileSystem, Path writerPath, Path dataPath) throws IOException {
        this(fileSystem, writerPath, dataPath, 5);
    }

    public FileWriter(FileSystem fileSystem, Path writerPath, Path dataPath, int maxNumberOfSegmentsPerWriter)
            throws IOException {
        //writerPath="/tmp",dataPath="/data"
        this.fileSystem = fileSystem;
        this.writerPath = writerPath;
        this.dataPath = dataPath;
        this.maxNumberOfSegmentsPerWriter = maxNumberOfSegmentsPerWriter;
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
    }

    public void commit() throws IOException {
        closeOutputStream();
        StringBuilder buffer = buffer(System.currentTimeMillis());
        buffer.append('_').append(writerPath.getName());
        fileSystem.rename(writerPath, new Path(dataPath, buffer.toString()));
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
        readerMore = false;
        reader = null;
        readers = null;
        //写真实数据的路径，往/tmp/b603152b-475a-4937-be6a-bfef1d7783d4/data中写数据
        currentOutputStreamPath = new Path(writerPath, UUID.randomUUID().toString());
        writer = new Index.Writer(fileSystem, new Configuration(fileSystem.getConf()), new Path(currentOutputStreamPath,
                client.HeDb.DATA), progress, metaData, blockSize);
    }

    private Collection<Reader> getReaders() throws IOException {
        List<Reader> readers = new ArrayList<Reader>();
        for (FileStatus status : fileSystem.listStatus(writerPath)) {
            Path path = status.getPath();
            if (fileSystem.exists(new Path(path, client.HeDb.DELETE)) || !fileSystem.exists(new Path(path, client.HeDb.DATA))) {//stupid windows hack....
                continue;
            }
            readers.add(new Index.Reader(fileSystem, new Path(path, client.HeDb.DATA), fileSystem.getConf()));
        }
        return readers;
    }

    public void closeOutputStream() throws IOException {
        if (writer != null) {
            flushReader();
            writer.close();
            if (reader != null) {
                reader.close();
                List<Path> readerPaths = reader.getReaderPaths();
                for (Path p : readerPaths) {
                    LOG.info("Removing [" + p + "] [" + delete(p) + "]");
                }
            }
            Path dst = new Path(writerPath, getFileName(firstKey, prevKey));
            if (fileSystem.rename(currentOutputStreamPath, dst)) {
                LOG.info("Moved [" + currentOutputStreamPath + "] to [" + dst + "]");
            } else {
                throw new IOException("Could not rename [" + currentOutputStreamPath + "] to [" + dst + "]");
            }
        }
    }

    private boolean delete(Path p) throws IOException {
        int deleteTries = 0;
        boolean result = false;
        while (!(result = fileSystem.delete(p, true)) && deleteTries < MAX_DELETE_RETRIES) {
            deleteTries++;
        }
        if (!result) {
            LOG.info("Marking [" + p + "] deleted");
            fileSystem.createNewFile(new Path(p, client.HeDb.DELETE));
        }
        return result;
    }

    private void flushReader() throws IOException {
        if (reader != null) {
            while (readerMore) {
                writer.write(readerKey, readerValue);
                readerMore = reader.next(readerKey, readerValue, Scanner.ALL);
            }
        }
    }

    private static String getFileName(Key firstKey, Key prevKey) {
        StringBuilder builder = new StringBuilder();
        Utils.addHexString(builder, firstKey.rowId.getBytes(), 0, firstKey.rowId.getLength());
        builder.append('_');
        Utils.addHexString(builder, prevKey.rowId.getBytes(), 0, prevKey.rowId.getLength());
        return builder.toString();
    }

    public Path getCurrentWritePath() {
        return this.currentOutputStreamPath;
    }

    private StringBuilder buffer(long timestamp) {
        String s = Long.toString(timestamp);
        StringBuilder builder = new StringBuilder();
        int length = client.HeDb.FILENAME_BUFFER_SIZE - s.length();
        for (int i = 0; i < length; i++) {
            builder.append('0');
        }
        builder.append(s);
        return builder;
    }
}