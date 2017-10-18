package client;

import file.Key;
import file.Value;

import java.io.IOException;
import java.util.Comparator;

public interface Scanner {

  public static final Comparator<Key> LASTEST = new Comparator<Key>() {
    @Override
    public int compare(Key o1, Key o2) {
      int rc = o1.rowId.compareTo(o2.rowId);
      if (rc == 0) {
        return o1.qualifier.compareTo(o2.qualifier);
      }
      return rc;
    }
  };
  public static final Comparator<Key> ALL = new Comparator<Key>() {
    @Override
    public int compare(Key o1, Key o2) {
      return o1.compareTo(o2);
    }
  };

  boolean next(Key key, Value value) throws IOException;

  boolean next(Key key, Value value, Comparator<Key> comparator) throws IOException;

  void close() throws IOException;

  long getBytesRead();

}