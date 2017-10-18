package client;

import file.Index;
import file.Key;
import file.Value;

import java.io.IOException;
import java.util.Collection;

public class MergeWriter extends Writer {
  
  private final Writer writer;
  private MergeReader reader;
  private Key readerKey = new Key();
  private Value readerValue = new Value();
  private boolean readerMore;
  
  public MergeWriter(Writer writer, Collection<Index.Reader> readers) throws IOException {
    this.writer = writer;
    reader = new MergeReader(readers,null,null);
    readerMore = reader.next(readerKey, readerValue, Scanner.ALL);
  }

  @Override
  public void write(Key key, Value value) throws IOException {
    while (readerMore) {
      //check if key should be written then get next from reader and re-check
      if (readerKey.compareTo(key) < 0) {
        writer.write(readerKey, readerValue);
        readerMore = reader.next(readerKey, readerValue, Scanner.ALL);
      } else {
        break;
      }
    }
    writer.write(key, value);
  }

  @Override
  public void rollback() throws IOException {
    writer.rollback();
    reader.close();
  }

  @Override
  public void commit() throws IOException {
    writer.commit();
    reader.close();
  }

}
