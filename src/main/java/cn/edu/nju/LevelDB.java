package cn.edu.nju;

import cn.edu.nju.client.*;
import cn.edu.nju.file.Index;
import cn.edu.nju.file.Key;
import cn.edu.nju.file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

public class LevelDB {

    public static final String DELETE = "delete";
    public static final String GENERATION = "generation";
    public static final String TMP = "tmp";
    public static final String DATA = "data";
    public static int maxBufferedElements = 10000;

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    private final Path rootPath;
    private final Path storePath;
    private final Path generationPath;
    private FileSystem fileSystem;
    private Writer writer;
    private Reader reader;
    private GenerationManager generation;
    private MyProcessor processor;

    /**
     * 默认hdfs地址，测试用
     *
     * @param databaseName
     * @param configuration
     * @throws IOException
     */
    public LevelDB(MyProcessor processor, Path databaseName, Configuration configuration) throws IOException {
        this(processor, databaseName, configuration, "hdfs://localhost:9000");
    }

    public LevelDB(MyProcessor processor, Path databaseName, Configuration configuration, String hdfsUrl) throws IOException {
        this.processor = processor;
        this.rootPath = new Path("/");
        this.storePath = new Path(rootPath, TMP);
        this.generationPath = new Path(rootPath, GENERATION);
        try {
            //init filesystem
            this.fileSystem = FileSystem.get(URI.create(hdfsUrl), configuration);
            createDatabaseIfMissing();
            refreshGeneration();
            ensureOpenWriter();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes the key and value into the database.
     *
     * @param key   the key.
     * @param value the value.
     */
    public void write(Key key, Value value) throws IOException {
        writer.write(key, value);
    }


    /**
     * 先判断内存中是否有该key，再去文件系统中找
     * @param key   the key.
     * @param value the value.
     * @return true if key was found, false if missing.
     */
    public boolean read(Key key, Value value) throws IOException {
        ensureOpenReader();
        return reader.read(key, value);
    }

    public boolean findInCache(Key key, Value value) {
        return ((BufferedWriter) writer).findInCache(key, value);
    }


    /**
     * Removes all changes since the last commit was called.
     */
    public void rollback() throws IOException {
        if (writer != null) {
            writer.rollback();
        }
        writer = null;
    }

    /**
     * Rolls back anything not committed and closes.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        rollback();
    }

    private void ensureOpenWriter() throws IOException {
        if (writer == null) {
            writer = new BufferedWriter(fileSystem, storePath, maxBufferedElements, this);
        }
    }

    private void ensureOpenReader() throws IOException {
        if (reader == null) {
            reader = new Reader(this, generation);
        }
    }

    /**
     * 第一次constructor调用，初始化generation；之后调用往hdfs中写入range索引
     * @throws IOException
     */
    public void refreshGeneration() throws IOException {
        if (generation != null) {
            generation.flush(fileSystem);
        } else {
            generation = new GenerationManager(fileSystem, generationPath);
        }
    }

    public void updateRange(Key startKey, Key endKey, Path dataPath) {
        updateRange(startKey, endKey, dataPath, true);
    }

    public void updateRange(Key startKey, Key endKey, Path dataPath, boolean ifNotify) {
        generation.addRange(startKey, endKey, dataPath);
        if (ifNotify) {
            processor.notifyAddRange(startKey, endKey, dataPath);
        }
    }

    private void createDatabaseIfMissing() throws IOException {
        fileSystem.mkdirs(storePath);
        fileSystem.mkdirs(generationPath);
    }

    public Index.Reader openIndex(Path path) throws IOException {
        return new Index.Reader(fileSystem, new Path(path, LevelDB.DATA), fileSystem.getConf());
    }

    public void updateFlushPath(String currentPath) {
        ((BufferedWriter)writer).setFlushPath(currentPath);
    }
}