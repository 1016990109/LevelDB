package client;


import file.Key;
import file.Value;

import java.io.IOException;

public abstract class Writer {
  public abstract void write(Key key, Value value) throws IOException;

  public abstract void rollback() throws IOException;

  public abstract void commit() throws IOException;
}
