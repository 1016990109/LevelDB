package file;

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
//      fileSystem.delete(indexPath, true);
    }

    public List<Entry<Key, Long>> getDensity(long minDensityPerSplit) throws IOException {
      reader.seek(initPos);
      Key k = new Key();
      LongWritable l = new LongWritable();

      List<Entry<Key, Long>> list = new ArrayList<Entry<Key, Long>>();
      long lastPosition = 0;
      long currentPosition = 0;
      while (reader.next(k, l)) {
        lastPosition = currentPosition;
        currentPosition = l.get();
        if (list.isEmpty()) {
          list.add(newEntry(k, currentPosition));
        } else {
          long value = currentPosition - lastPosition;
          list.add(newEntry(k, value));
        }
      }

      long total = 0;
      for (Entry<Key, Long> e : list) {
        total += e.getValue();
      }

      if (total < minDensityPerSplit) {
        return new ArrayList<Entry<Key, Long>>();
      }

      return list;
    }

    public void delete() {
      if (indexReader != null) {
        indexReader.delete();
      }
    }
  }

  public static Entry<Key, Long> newEntry(Key k, final long value) {
    final Key key = new Key();
    key.set(k);
    return new Entry<Key, Long>() {

      @Override
      public Long setValue(Long value) {
        throw new RuntimeException();
      }

      @Override
      public Long getValue() {
        return value;
      }

      @Override
      public Key getKey() {
        return key;
      }

      @Override
      public String toString() {
        return "{" + getKey() + "}={" + getValue() + "}";
      }
    };
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
    private boolean closed;
    private long dataLength;
    private AtomicLong bytesRead = new AtomicLong();
    private final Path path;

    public Reader(FileSystem fileSystem, Path path, Configuration conf) throws IOException {
      reader = new SequenceFile.Reader(fileSystem, path, conf) {
        @Override
        protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize, long length) throws IOException {
          return new FSDataInputStream(new CounterInputStream(bytesRead, super.openFile(fs, file, bufferSize, length)));
        }
      };
      this.path = path;
      dataLength = fileSystem.getFileStatus(path).getLen();
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
      // move forward in data file to correct position
      for (more = reader.next(pk, pv); more && pk.compareTo(key) <= 0; k.set(pk), v.set(pv), more = reader.next(pk, pv)) {
        // do nothing
      }
      prevKV = true;
      bytesRead.set(0);
      return more || pk.compareTo(key) == 0;
    }

    public boolean next() throws IOException {
      if (prevKV) {
        //读前一个，假装读
        pk.set(k);
        pv.set(v);
        prevKV = false;
        return more;
      }
      more = reader.next(k, v);
      return more;
    }

    public void fetch(Key key, Value value) {
//      if (prevKV) {
//        throw new RuntimeException("next() needs to be called before a fetch.");
//      }
      key.set(k);
      value.set(v);
    }

    public void fetchPrev(Key key, Value value) {
      key.set(pk);
      value.set(pv);
    }

    public void close() throws IOException {
      closed = true;
      reader.close();
      indexReader.close();
    }

    @Override
    public int compareTo(Reader o) {
      return k.compareTo(o.k);
    }

    public boolean isAtEnd() {
      return !more && !prevKV;
    }

    public boolean isClosed() {
      return closed;
    }

    public List<Entry<Key, Long>> getDensity(long minDensityPerSplit) throws IOException {
      List<Entry<Key, Long>> density = indexReader.getDensity(minDensityPerSplit);
      if (density.isEmpty()) {
        reader.seek(reset);
        Key key = new Key();
        reader.next(key);
        density.add(newEntry(key, dataLength));
      }
      return density;
    }

    public long getBytesRead() {
      return bytesRead.get();
    }

    public Path getPath() {
      return path;
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

  private static class CounterInputStream extends InputStream implements Seekable, PositionedReadable {

    private FSDataInputStream in;
    private AtomicLong bytesRead;

    public CounterInputStream(AtomicLong bytesRead, FSDataInputStream in) {
      this.in = in;
      this.bytesRead = bytesRead;
    }

    public final int read(byte[] b) throws IOException {
      return add(in.read(b));
    }

    private int add(int val) {
      bytesRead.addAndGet(val);
      return val;
    }

    private void incr() {
      bytesRead.incrementAndGet();
    }

    public int read() throws IOException {
      incr();
      return in.read();
    }

    public void seek(long desired) throws IOException {
      in.seek(desired);
    }

    public long getPos() throws IOException {
      return in.getPos();
    }

    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return add(in.read(position, buffer, offset, length));
    }

    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      add(length);
      in.readFully(position, buffer, offset, length);
    }

    public void readFully(long position, byte[] buffer) throws IOException {
      add(buffer.length);
      in.readFully(position, buffer);
    }

    public boolean seekToNewSource(long targetPos) throws IOException {
      return in.seekToNewSource(targetPos);
    }

    public final int read(byte[] b, int off, int len) throws IOException {
      return add(in.read(b, off, len));
    }

    public long skip(long n) throws IOException {
      return in.skip(n);
    }

    public int available() throws IOException {
      return in.available();
    }

    public void close() throws IOException {
      in.close();
    }

    public void mark(int readlimit) {
      in.mark(readlimit);
    }

    public void reset() throws IOException {
      in.reset();
    }

    public boolean markSupported() {
      return in.markSupported();
    }
  }
}
