package cn.edu.nju.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class Index {

  private Index() {

  }

  static class IndexReader {

    private IndexReader indexReader;
    private SequenceFile.Reader reader;
    private Key bufKey = new Key();
    private LongWritable bufPosition = new LongWritable();
    private final long initPos;
    private final Path indexPath;
    private final FileSystem fileSystem;

    public IndexReader(FileSystem fileSystem, Path path, Configuration conf, int index) throws IOException {
      this.fileSystem = fileSystem;
      indexPath = getIndexFile(path, index);
      if (fileSystem.exists(indexPath)) {
        reader = new SequenceFile.Reader(fileSystem, indexPath, conf);
        initPos = reader.getPosition();
      } else {
        initPos = -1l;
      }
      Path nextIndexPath = getIndexFile(path, index + 1);
      if (fileSystem.exists(nextIndexPath)) {
        indexReader = new IndexReader(fileSystem, path, conf, index + 1);
      }
    }

    public long getLocation(Key key) throws IOException {
      if (reader == null) {
        return -1l;
      }
      if (indexReader != null) {
        long location = indexReader.getLocation(key);
        if (location < 0) {
          reader.seek(initPos);
        } else {
          reader.seek(location);
        }
      } else {
        reader.seek(initPos);
      }
      long p = -1;
      boolean more;
      do {
        more = reader.next(bufKey, bufPosition);
        int compareTo = bufKey.compareTo(key);
        if (compareTo > 0) {
          return p;
        } else if (compareTo == 0) {
          return bufPosition.get();
        } else {
          p = bufPosition.get();
        }
      } while (more);
      return p;
    }

    public void close() throws IOException {
      if (indexReader != null) {
        indexReader.close();
      }

      reader.close();
    }
  }

  public static class Reader implements Comparable<Reader> {

    private SequenceFile.Reader reader;
    private IndexReader indexReader;
    private long reset;
    private Key k = new Key();
    private Key pk = new Key();
    private Value v = new Value();
    private Value pv = new Value();
    private boolean more = true;
    private boolean prevKV;
    private AtomicLong bytesRead = new AtomicLong();

    public Reader(FileSystem fileSystem, Path path, Configuration conf) throws IOException {
      reader = new SequenceFile.Reader(fileSystem, path, conf);
      reset = reader.getPosition();
      indexReader = new IndexReader(fileSystem, path, conf, 0);
    }

    public boolean seek(Key key) throws IOException {
      long position = indexReader.getLocation(key);
      if (position < 0L) {
        reader.seek(reset);
      } else {
        reader.seek(position);
      }
      // move position
      for (more = reader.next(pk, pv); more && pk.compareTo(key) <= 0; k.set(pk), v.set(pv), more = reader.next(pk, pv)) {
        // do nothing
      }
      prevKV = true;
      bytesRead.set(0);
      return more || pk.compareTo(key) == 0;
    }

    public boolean next() throws IOException {
      if (prevKV) {
        pk.set(k);
        pv.set(v);
        prevKV = false;
        return more;
      }
      more = reader.next(k, v);
      return more;
    }

    public void fetch(Key key, Value value) {
      key.set(k);
      value.set(v);
    }

    public void fetchPrev(Key key, Value value) {
      key.set(pk);
      value.set(pv);
    }

    public void close() throws IOException {
      reader.close();
      indexReader.close();
    }

    @Override
    public int compareTo(Reader o) {
      return k.compareTo(o.k);
    }
  }

  static class IndexWriter {

    private IndexWriter indexWriter;
    private SequenceFile.Writer writer;
    private final int interval = 128;
    private int count = 0;
    private LongWritable position = new LongWritable();
    private Path dataFile;
    private FileSystem fileSystem;
    private Progressable progress;
    private Configuration conf;
    private int level;
    private int blockSize;

    public IndexWriter(FileSystem fileSystem, Configuration conf, Path dataFile, Progressable progress, int blockSize, int level) throws IOException {
      this.fileSystem = fileSystem;
      this.conf = conf;
      this.dataFile = dataFile;
      this.progress = progress;
      this.level = level;
      this.blockSize = blockSize;
    }

    public void write(Key key, LongWritable pos) throws IOException {
      ensureOpen();
      if (count >= interval) {
        ensureIndexOpen();
        position.set(writer.getLength());
        indexWriter.write(key, position);
        count = 0;
      }
      writer.append(key, pos);
      count++;
    }

    public void close() throws IOException {
      if (indexWriter != null) {
        indexWriter.close();
      }
      if (writer != null) {
        writer.close();
      }
    }

    //多级索引
    private void ensureIndexOpen() throws IOException {
      if (indexWriter == null) {
        indexWriter = new IndexWriter(fileSystem, conf, dataFile, progress, blockSize, level + 1);
      }
    }

    private void ensureOpen() throws IOException {
      if (writer == null) {
        writer = getWriter(true, fileSystem, conf, getIndexFile(dataFile, level), progress, new Metadata());
      }
    }
  }

  public static class Writer {

    private IndexWriter indexWriter;
    private SequenceFile.Writer writer;
    private final int interval = 128;
    private int count = 0;
    private LongWritable position = new LongWritable();

    public Writer(FileSystem fileSystem, Configuration conf, Path dataFile, Progressable progress, Metadata metaData, int blockSize) throws IOException {
      //dataFile=“/tmp/b603152b-475a-4937-be6a-bfef1d7783d4/data”
      conf.setInt("io.seqfile.compress.blocksize", blockSize);
      writer = getWriter(false, fileSystem, conf, dataFile, progress, metaData);
      //同时建立索引writer
      indexWriter = new IndexWriter(fileSystem, conf, dataFile, progress, blockSize, 0);
    }

    public void write(Key key, Value value) throws IOException {
      if (count >= interval) {
        writer.sync();
        position.set(writer.getLength());
        //写索引，如果写interval个数据还没到节点挂掉那么数据已经写入hdfs但是却没有索引是脏数据，重启节点后会重新写
        indexWriter.write(key, position);
        count = 0;
      }
      writer.append(key, value);
      count++;
    }

    public void close() throws IOException {
      indexWriter.close();
      writer.close();
    }

  }

  private static SequenceFile.Writer getWriter(boolean index, FileSystem fileSystem, Configuration conf, Path dataFile, Progressable progress, Metadata metaData)
      throws IOException {
    CompressionType type = CompressionType.NONE;
    if (index) {
      return SequenceFile.createWriter(fileSystem, conf, dataFile, Key.class, LongWritable.class, CompressionType.NONE, getCodec(conf), progress, metaData);
    } else {
      return SequenceFile.createWriter(fileSystem, conf, dataFile, Key.class, Value.class, type, getCodec(conf), progress, metaData);
    }
  }

  private static CompressionCodec getCodec(Configuration conf) {
    if (SnappyCodec.isNativeCodeLoaded()) {
      return new SnappyCodec();
    }
    return new DeflateCodec();
  }

  public static Path getIndexFile(Path dataFile, int index) {
    return new Path(dataFile.getParent(), dataFile.getName() + ".index." + index);
  }
}
