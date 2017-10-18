package client;

import file.Key;
import file.Value;

import java.io.IOException;
import java.util.Arrays;

public class BufferedWriter extends Writer {

  private final Writer writer;
  private final KeyValueEntry[] buffer;
  private final int maxSize;
  private int count = 0;
  private HeDb client;

  public BufferedWriter(Writer writer, int maxSize, HeDb client) {
    this.writer = writer;
    this.maxSize = maxSize;
    this.client = client;
    buffer = new KeyValueEntry[maxSize];
  }

  // TODO: 10/17/17 need log
  @Override
  public void write(Key key, Value value) throws IOException {
    KeyValueEntry entry = new KeyValueEntry();
    entry.key = clone(key);
    entry.value = clone(value);
    buffer[count++] = entry;
    flushIfNeeded();
  }

  private Value clone(Value value) {
    Value v = new Value();
    v.set(value);
    return v;
  }

  private Key clone(Key key) {
    Key k = new Key();
    k.set(key);
    return k;
  }

  private void flushIfNeeded() throws IOException {
    if (count >= maxSize) {
      flush();
    }
  }

  private void flush() throws IOException {
    if (count == 0) {
      return;
    }
    Arrays.sort(buffer, 0, count);
    for (int i = 0; i < count; i++) {
      KeyValueEntry entry = buffer[i];
      writer.write(entry.key, entry.value);
    }
    this.client.updateRange(buffer[0].key, buffer[count-1].key, ((FileWriter) writer).getCurrentWritePath());
    count = 0;
    //todo 并且写range
    this.client.refresh();
  }

  @Override
  public void rollback() throws IOException {
    count = 0;
    writer.rollback();
  }

  @Override
  public void commit() throws IOException {
    flush();
    writer.commit();
  }

  static class KeyValueEntry implements Comparable<KeyValueEntry> {
    Key key;
    Value value;

    @Override
    public int compareTo(KeyValueEntry o) {
      return key.compareTo(o.key);
    }
  }

}
