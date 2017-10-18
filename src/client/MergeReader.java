package client;

import file.Index;
import file.Index.Reader;
import file.Key;
import file.Value;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.*;

public class MergeReader {

  private final Index.Reader[] readersArray;
  private final int size;
  private final Key endKey;
  private Key lastKey = new Key();
  private Key workingKey = new Key();
  private Value workingValue = new Value();

  public MergeReader(Collection<Index.Reader> readers, Key startKey, Key endKey) throws IOException {
    this.endKey = endKey;
    this.size = readers.size();
    this.readersArray = new Index.Reader[size];
    int i = 0;
    for (Index.Reader reader : readers) {
      readersArray[i++] = reader;
    }
    for (Index.Reader reader : readersArray) {
      if (startKey == null) {
        reader.next();
      } else {
        reader.seek(startKey);
      }
    }
    Arrays.sort(readersArray);
  }

  public boolean next(Key key, Value value, Comparator<Key> comparator) throws IOException {
    LOOP: while (true) {
      for (int i = 0; i < size; i++) {
        Index.Reader reader = readersArray[i];
        if (!reader.isAtEnd() && !reader.isClosed()) {
          reader.fetch(workingKey, workingValue);
          if (endKey != null && comparator.compare(workingKey, endKey) >= 0) {
            reader.close();
            continue LOOP;
          }
          if (comparator.compare(workingKey, lastKey) == 0) {
            reader.next();
            sort(i);
            continue LOOP;
          }
          lastKey.set(workingKey);
          key.set(workingKey);
          value.set(workingValue);
          reader.next();
          sort(i);
          return true;
        }
      }
      return false;
    }
  }

  private void sort(int index) {
    for (int i = index + 1; i < size; i++) {
      Reader current = readersArray[i - 1];
      Reader next = readersArray[i];
      if (current.compareTo(next) < 0) {
        return;
      }
      readersArray[i - 1] = next;
      readersArray[i] = current;
    }
  }

  public List<Path> getReaderPaths() {
    List<Path> paths = new ArrayList<Path>();
    for (Index.Reader reader : readersArray) {
      paths.add(reader.getPath().getParent());
    }
    return paths;
  }

  public long getBytesRead() {
    long total = 0;
    for (int i = 0; i < size; i++) {
      total += readersArray[i].getBytesRead();
    }
    return total;
  }

  public void close() throws IOException {
    for (Index.Reader reader : readersArray) {
      reader.close();
    }
  }

}
