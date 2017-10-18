package client;

import file.Index;
import file.Key;
import file.Value;

import java.io.IOException;
import java.util.Iterator;

public class Reader {

  private final GenerationManager generation;
  private final HeDb client;
  private final Key k = new Key();
  private final Value v = new Value();

  public Reader(HeDb client, GenerationManager generation) {
    this.client = client;
    this.generation = generation;
  }

  public boolean read(Key key, Value value) throws IOException {
    Iterable<Range> iterable = generation.findAllRangesThatContainKey(key);
    Iterator<Range> iterator = iterable.iterator();
    while (iterator.hasNext()) {
      Range r = iterator.next();
      Index.Reader reader = client.openIndex(r.getPath());
      try {
        if (reader.seek(key)) {
          if (reader.next()) {
            reader.fetch(k, v);
          } else {
            reader.fetchPrev(k, v);
          }

          if (k.compareTo(key) == 0) {
            key.set(k);
            value.set(v);
            return true;
          }
        }
      } finally {
        reader.close();
      }
    }
    return false;
  }

}
