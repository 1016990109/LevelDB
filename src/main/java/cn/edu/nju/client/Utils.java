package cn.edu.nju.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Utils {

  public static void writeString(DataOutput out, String str) throws IOException {
    byte[] bs = str.getBytes();
    org.apache.hadoop.record.Utils.writeVInt(out, bs.length);
    out.write(bs);
  }

  public static String readString(DataInput in) throws IOException {
    int length = org.apache.hadoop.record.Utils.readVInt(in);
    byte[] buffer = new byte[length];
    in.readFully(buffer);
    return new String(buffer);
  }
}