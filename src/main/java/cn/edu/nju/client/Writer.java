package cn.edu.nju.client;


import cn.edu.nju.file.Key;
import cn.edu.nju.file.Value;

import java.io.IOException;

public abstract class Writer {
  public abstract void write(Key key, Value value) throws IOException;

  public abstract void rollback() throws IOException;
}
