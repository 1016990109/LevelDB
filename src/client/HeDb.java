package client;

import file.Index;
import file.Key;
import file.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

public class HeDb {

  public static final String HEDB_ROOT_PATH = "hadoop.tmp.dir";
  public static final String HEDB_DATABASE_PATH = "hedb.database.path";
  public static final String DELETE = "delete";
  public static final String GENERATION = "generation";
  public static final String TMP = "tmp";
  public static final String SESSION = "session";
  public static final String DATA = "data";
  public static final int FILENAME_BUFFER_SIZE = 18;
  public static final long MAX_SESSION_TIMEOUT = TimeUnit.MINUTES.toMillis(10);
  public static final long SESSION_UPDATE = TimeUnit.SECONDS.toMillis(30);
  public static final String HEDB_SESSION_ID = "hedb.sessionid";
  public static final String HEDB_SPLIT_SIZE = "hedb.split.size";
  public static final String HEDB_SPLIT_NUMBER = "hedb.split.number";
  private static final String HDFS_PATH = "hdfs://master:9000";

  static {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
  }

  private final Path rootPath;
  private final Path dataPath;
  private final Path storePath;
  private final Path generationPath;
  private FileSystem fileSystem;
  private Writer writer;
  private Reader reader;
  private GenerationManager generation;
  private int maxBufferedElements = 1000;
  private int maxNumberOfSegmentsPerWriter = 5;
  private long minDensityPerSplit = 1024 * 1024;

  /**
   * 
   * Creates the {@HeClient} object to interact with the database.
   * 
   * @param configuration
   * @param databaseName name
   * @throws IOException
   */

  public HeDb(Path databaseName, Configuration configuration) throws IOException {
    this.rootPath = new Path("/");
    this.dataPath = new Path(rootPath, DATA);
    this.storePath = new Path(rootPath, TMP);
    this.generationPath = new Path(rootPath, GENERATION);
    try {
      //init filesystem
      this.fileSystem = FileSystem.get(URI.create("hdfs://master:9000"), new Configuration());
      createDatabaseIfMissing();
      refresh();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Writes the key and value into the database.
   * 
   * @param key
   *          the key.
   * @param value
   *          the value.
   */
  public void write(Key key, Value value) throws IOException {
    ensureOpenWriter();
    writer.write(key, value);
  }


  /**
   * Reads the key and value from the database.
   * 
   * @param key
   *          the key.
   * @param value
   *          the value.
   * @return true if key was found, false if missing.
   */
  public boolean read(Key key, Value value) throws IOException {
    ensureOpenReader();
    return reader.read(key, value);
  }

  /**
   * Updates to the most current view of the database.
   */
  public void refresh() throws IOException {
    refreshGeneration();
  }

  /**
   * Commits all writes to the database. The last key to be committed wins.
   */
  public void commit() throws IOException {
    if (writer == null && fileSystem.exists(new Path(storePath, ""))) {
      ensureOpenWriter();
    }
    if (writer != null) {
      writer.commit();
    }
    writer = null;
    refresh();
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
      writer = new BufferedWriter(new FileWriter(fileSystem, storePath, dataPath, maxNumberOfSegmentsPerWriter), maxBufferedElements, this);
    }
  }

  private void ensureOpenReader() throws IOException {
    if (reader == null) {
      reader = new Reader(this, generation);
    }
  }

  private void refreshGeneration() throws IOException {
    //初始化时需要读取所有的range索引，多个索引存一个文件，可能会有多个文件，文件名暂时随即生成，因为将读出全部索引
    if (generation != null) {
      generation.flush(fileSystem);
    }
    generation = new GenerationManager(fileSystem, generationPath, dataPath);
  }

  public void updateRange(Key startKey, Key endKey, Path dataPath) {
    generation.addRange(startKey, endKey, dataPath);
  }

  private void createDatabaseIfMissing() throws IOException {
    fileSystem.mkdirs(dataPath);
    fileSystem.mkdirs(storePath);
    fileSystem.mkdirs(generationPath);
  }

  public Index.Reader openIndex(Path path) throws IOException {
    return new Index.Reader(fileSystem, new Path(path, HeDb.DATA), fileSystem.getConf());
  }

  public List<Entry<Key, Long>> getSplits(long numberOfPartitions) throws IOException {
    List<Range> allRanges = generation.findAllRangesThatContainKey();
    long totalSize = 0;
    for (Range range : allRanges) {
      ContentSummary contentSummary = fileSystem.getContentSummary(range.getPath());
      totalSize += contentSummary.getLength();
    }
    long densityPerSplit = totalSize / numberOfPartitions;

    List<Entry<Key, Long>> keysAndSizes = new ArrayList<Entry<Key, Long>>();
    for (Range range : allRanges) {
      Path path = range.getPath();
      Index.Reader index = openIndex(path);
      keysAndSizes.addAll(index.getDensity(minDensityPerSplit));
      index.close();
    }

    Collections.sort(keysAndSizes, new Comparator<Entry<Key, Long>>() {
      @Override
      public int compare(Entry<Key, Long> o1, Entry<Key, Long> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });

    List<Entry<Key, Long>> result = new ArrayList<Entry<Key, Long>>();
    long total = 0;
    for (Entry<Key, Long> e : keysAndSizes) {
      if (total >= densityPerSplit) {
        result.add(Index.newEntry(e.getKey(), total));
        total = 0;
      }
      total += e.getValue();
    }

    return result;
  }

  public List<Entry<Key, Long>> getSplitsBySize(long splitSize) throws IOException {
    return getSplits(getSize() / splitSize);
  }

  public long getSize() throws IOException {
    List<Range> allRanges = generation.findAllRangesThatContainKey();
    long totalSize = 0;
    for (Range range : allRanges) {
      ContentSummary contentSummary = fileSystem.getContentSummary(range.getPath());
      totalSize += contentSummary.getLength();
    }
    return totalSize;
  }

  public GenerationManager getGeneration() {
    return generation;
  }

  public int getMaxBufferedElements() {
    return maxBufferedElements;
  }

  public void setMaxBufferedElements(int maxBufferedElements) {
    this.maxBufferedElements = maxBufferedElements;
  }

}