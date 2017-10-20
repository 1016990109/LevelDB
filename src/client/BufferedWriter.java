package client;

import file.Key;
import file.Value;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class BufferedWriter extends Writer {

  private final Writer writer;
  private ConcurrentSkipListMap<Key, Value> buffer;
  private final int maxSize;
  private HeDb client;

  public BufferedWriter(Writer writer, int maxSize, HeDb client) {
    this.writer = writer;
    this.maxSize = maxSize;
    this.client = client;
    buffer = new ConcurrentSkipListMap<>();
  }

  // TODO: 10/17/17 need log
  @Override
  public void write(Key key, Value value) throws IOException {
    buffer.put(key, value);
    client.writeLog(key, value);
    flushIfNeeded();
  }

  /**
   * 从log中读取数据恢复时用，不用再写log
   * @param key
   * @param value
   * @throws IOException
   */
  public void restore(Key key, Value value) throws IOException {
    buffer.put(key, value);
    flushIfNeeded();
  }

  private void flushIfNeeded() throws IOException {
    if (buffer.size() >= maxSize) {
      flush();
    }
  }

  private void flush() throws IOException {
    if (buffer.size() == 0) {
      return;
    }
    for (Map.Entry<Key, Value> entry: buffer.entrySet()) {
      writer.write(entry.getKey(), entry.getValue());
    }

    this.client.updateRange(buffer.firstKey(), buffer.lastKey(), ((FileWriter) writer).getCurrentWritePath());
    this.client.refresh();
  }

  @Override
  public void rollback() throws IOException {
    buffer = new ConcurrentSkipListMap<>();
    writer.rollback();
  }
}
